WITH
SQ_claim_transaction_Reserves AS (
	SELECT 
	       CT.claim_trans_id,
	       CT.claim_trans_ak_id,
	       CT.claimant_cov_det_ak_id,
	       CT.claim_pay_ak_id, 
	       CT.cause_of_loss,
	       CT.reserve_ctgry,
	       CT.type_disability,
	       CT.sar_id,
	       CT.financial_type_code,
	       CT.s3p_trans_code,
	       CT.pms_trans_code,
	       CT.trans_code,
	       CT.trans_date,
	       CT.pms_acct_entered_date,
	       CT.trans_base_type_code,
	       CT.trans_ctgry_code,
	       CT.trans_amt,
	       CT.trans_hist_amt,
	       CT.draft_num,
	       CT.source_sys_id,
	       CT.trans_offset_onset_ind,
	      @{pipeline().parameters.DATESQL} as LastDayOfLastMonth
	FROM   
	       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction CT
	WHERE CT.err_flag > = 0
	and (CT.pms_trans_code in ('95','97','98','99') or (CT.pms_trans_code='90' and CT.trans_offset_onset_ind='N/A'))
	and not CT.financial_type_code + CT.s3p_trans_code in ('E66','E65','B65','B66','R65','R66','S65','S66')
	and UPPER(CT.source_sys_id)='EXCEED'
	and CT.crrnt_snpsht_flag = 1
	and CT.trans_date <= @{pipeline().parameters.DATESQL}
	--debug
	--and CT.claimant_cov_det_ak_id=-10
),
EXP_Default_Reserves AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	financial_type_code,
	s3p_trans_code,
	pms_trans_code,
	trans_code,
	trans_date,
	pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	-- *INF*: financial_type_code || s3p_trans_code  ||  IIF(trans_ctgry_code = 'N/A','',trans_ctgry_code)
	financial_type_code || s3p_trans_code || IFF(trans_ctgry_code = 'N/A', '', trans_ctgry_code) AS v_Exceed_Trans_Code,
	v_Exceed_Trans_Code AS Exceed_Trans_Code,
	trans_amt,
	trans_hist_amt,
	draft_num,
	source_sys_id,
	trans_offset_onset_ind,
	LastDayOfLastMonth
	FROM SQ_claim_transaction_Reserves
),
SQ_claim_transaction_Reserves_91_92 AS (
	-- top selection is for 91's that have the close transaction occur past the end of month.
	-- the other selection is all 92s with rules that will occur later as to whether they are truly 92s.
	
	SELECT CT.claim_trans_id,
	       CT.claim_trans_ak_id,
	       CT.claimant_cov_det_ak_id,
	       CT.claim_pay_ak_id, 
	       CT.cause_of_loss,
	       CT.reserve_ctgry,
	       CT.type_disability,
	       CT.sar_id,
	       CT.financial_type_code,
	       CT.s3p_trans_code,
	       CT.pms_trans_code,
	       CT.trans_code,
	       CT.trans_date,
	       CT.pms_acct_entered_date,
	       CT.trans_base_type_code,
	       CT.trans_ctgry_code,
	       CT.trans_amt,
	       CT.trans_hist_amt,
	       CT.draft_num,
	       CT.source_sys_id,
	       CT.trans_offset_onset_ind,
	       @{pipeline().parameters.DATESQL} as LastDayOfLastMonth 
	FROM vw_claim_transaction CT
	WHERE CT.err_flag > = 0  
	and CT.pms_trans_code in ('91')
	and not CT.financial_type_code + CT.s3p_trans_code in ('E66','E65','B65','B66','R65','R66','S65','S66')
	and UPPER(CT.source_sys_id)='EXCEED'
	and CT.crrnt_snpsht_flag = 1
	and CT.trans_date <= @{pipeline().parameters.DATESQL}
	and CT.modified_date > @{pipeline().parameters.DATESQL}
	--debug
	--and CT.claimant_cov_det_ak_id=2015624and CT.financial_type_code='D'
	
	union all
	
	SELECT CT.claim_trans_id,
	       CT.claim_trans_ak_id,
	       CT.claimant_cov_det_ak_id,
	       CT.claim_pay_ak_id, 
	       CT.cause_of_loss,
	       CT.reserve_ctgry,
	       CT.type_disability,
	       CT.sar_id,
	       CT.financial_type_code,
	       CT.s3p_trans_code,
	       CT.pms_trans_code,
	       CT.trans_code,
	       CT.trans_date,
	       CT.pms_acct_entered_date,
	       CT.trans_base_type_code,
	       CT.trans_ctgry_code,
	       CT.trans_amt,
	       CT.trans_hist_amt,
	       CT.draft_num,
	       CT.source_sys_id,
	       CT.trans_offset_onset_ind,
	       @{pipeline().parameters.DATESQL} as LastDayOfLastMonth
	FROM vw_claim_transaction CT
	WHERE CT.err_flag > = 0  
	and CT.pms_trans_code in ('92')
	and not CT.financial_type_code + CT.s3p_trans_code in ('E66','E65','B65','B66','R65','R66','S65','S66')
	and UPPER(CT.source_sys_id)='EXCEED'
	and CT.crrnt_snpsht_flag = 1
	and CT.trans_date <= @{pipeline().parameters.DATESQL}
	--debug
	--and CT.claimant_cov_det_ak_id=2015624and CT.financial_type_code='D'
	
	union all
	
	SELECT CT.claim_trans_id,
	       CT.claim_trans_ak_id,
	       CT.claimant_cov_det_ak_id,
	       CT.claim_pay_ak_id, 
	       CT.cause_of_loss,
	       CT.reserve_ctgry,
	       CT.type_disability,
	       CT.sar_id,
	       CT.financial_type_code,
	       CT.s3p_trans_code,
	       CT.pms_trans_code,
	       CT.trans_code,
	       CT.trans_date,
	       CT.pms_acct_entered_date,
	       CT.trans_base_type_code,
	       CT.trans_ctgry_code,
	       CT.trans_amt,
	       CT.trans_hist_amt,
	       CT.draft_num,
	       CT.source_sys_id,
	       CT.trans_offset_onset_ind,
	       @{pipeline().parameters.DATESQL} as LastDayOfLastMonth 
	FROM vw_claim_transaction CT with (nolock)
	WHERE CT.err_flag > = 0  
	and (CT.pms_trans_code='90' and CT.trans_offset_onset_ind='N')
	and not CT.financial_type_code + CT.s3p_trans_code in ('E66','E65','B65','B66','R65','R66','S65','S66')
	and UPPER(CT.source_sys_id)='EXCEED'
	and CT.crrnt_snpsht_flag = 1
	and CT.trans_date <= @{pipeline().parameters.DATESQL}
),
EXP_Default_Reserves_91_92 AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	financial_type_code,
	s3p_trans_code,
	pms_trans_code,
	trans_code,
	trans_date,
	pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	-- *INF*: financial_type_code || s3p_trans_code  ||  IIF(trans_ctgry_code = 'N/A','',trans_ctgry_code)
	financial_type_code || s3p_trans_code || IFF(trans_ctgry_code = 'N/A', '', trans_ctgry_code) AS V_Exceed_Trans_Code,
	V_Exceed_Trans_Code AS Exceed_Trans_Code,
	trans_amt,
	trans_hist_amt,
	draft_num,
	source_sys_id,
	trans_offset_onset_ind,
	LastDayOfLastMonth
	FROM SQ_claim_transaction_Reserves_91_92
),
mplt_loss_master_90N_91_92_pms_trans_code_conversions AS (WITH
	INPUT_90N_91_92_conversion AS (
		
	),
	EXP_Default_Reserves_91_92 AS (
		SELECT
		claimant_cov_det_ak_id,
		financial_type_code,
		pms_trans_code,
		trans_offset_onset_ind,
		LastDayOfLastMonth,
		-- *INF*: DECODE(true,
		-- pms_trans_code='90','O',
		-- pms_trans_code='91' and trans_offset_onset_ind='N','O',
		-- trans_offset_onset_ind
		-- )
		-- 
		-- -- we need to rig the lookup to look for the matching offset/onset indicator (N,O,N/A).  Since a 90 in this pipeline is an N we need to change it to be an O so we can find the 41/42 offset to verify we have an offset -- otherwise we risk the onset 90 and offset 91 reverting to a 90 and causing double reserves.
		-- -- also a 91N means we will have a 91O and 91N.  if we have no 41/42 then we risk two reserves so appling same logic as 90Ns.
		DECODE(
		    true,
		    pms_trans_code = '90', 'O',
		    pms_trans_code = '91' and trans_offset_onset_ind = 'N', 'O',
		    trans_offset_onset_ind
		) AS trans_offset_onset_ind_for_lookup
		FROM INPUT_90N_91_92_conversion
	),
	LKP_claim_transaction_41_42_66_22_pre_month_end AS (
		SELECT
		trans_date,
		pms_trans_code,
		claimant_cov_det_ak_id_IN,
		financial_type_code_IN,
		trans_offset_onset_ind_for_lookup,
		LastDayOfLastMonth,
		claimant_cov_det_ak_id,
		financial_type_code,
		trans_offset_onset_ind
		FROM (
			SELECT 
			claim_transaction.trans_date as trans_date, 
			claim_transaction.pms_trans_code as pms_trans_code, 
			claim_transaction.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
			claim_transaction.financial_type_code as financial_type_code, 
			claim_transaction.trans_offset_onset_ind as trans_offset_onset_ind 
			FROM 
			claim_transaction with (nolock)
			where 
			pms_trans_code in ('41','42','66','22')
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,financial_type_code,trans_offset_onset_ind,trans_date ORDER BY trans_date DESC) = 1
	),
	LKP_claim_transaction_41_42_22_post_month_end AS (
		SELECT
		trans_date,
		pms_trans_code,
		claimant_cov_det_ak_id_IN,
		financial_type_code_IN,
		LastDayOfLastMonth,
		claimant_cov_det_ak_id,
		financial_type_code
		FROM (
			SELECT 
			claim_transaction.trans_date as trans_date, 
			claim_transaction.pms_trans_code as pms_trans_code, 
			claim_transaction.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
			claim_transaction.financial_type_code as financial_type_code, 
			claim_transaction.trans_offset_onset_ind as trans_offset_onset_ind 
			FROM 
			claim_transaction with (nolock)
			where 
			pms_trans_code in ('41','42','22')
			and trans_offset_onset_ind <> 'O'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,financial_type_code,trans_date ORDER BY trans_date) = 1
	),
	LKP_claim_transaction_66_trans_for_92s AS (
		SELECT
		trans_date,
		pms_trans_code,
		claimant_cov_det_ak_id_IN,
		financial_type_code_IN,
		claimant_cov_det_ak_id,
		financial_type_code
		FROM (
			SELECT 
			claim_transaction.trans_date as trans_date, 
			claim_transaction.pms_trans_code as pms_trans_code, 
			claim_transaction.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
			claim_transaction.financial_type_code as financial_type_code
			FROM 
			claim_transaction with (nolock)
			where
			pms_trans_code = '66'
			and trans_offset_onset_ind <> 'O'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,financial_type_code ORDER BY trans_date DESC) = 1
	),
	EXP_check_transcode_lkp_output AS (
		SELECT
		LKP_claim_transaction_41_42_66_22_pre_month_end.trans_date AS lkp_trans_date,
		LKP_claim_transaction_41_42_66_22_pre_month_end.pms_trans_code AS lkp_pms_trans_code,
		-- *INF*: IIF(ISNULL(lkp_pms_trans_code),'N/A',lkp_pms_trans_code)
		IFF(lkp_pms_trans_code IS NULL, 'N/A', lkp_pms_trans_code) AS v_lkp_pms_trans_code,
		v_lkp_pms_trans_code AS lkp_pms_trans_code_pre_month_end,
		-- *INF*: DECODE(true,
		-- IN(v_lkp_pms_trans_code,'41','42','22'),'Y',
		-- v_lkp_pms_trans_code='N/A','N/A',
		-- 'N'
		-- )
		DECODE(
		    true,
		    v_lkp_pms_trans_code IN ('41','42','22'), 'Y',
		    v_lkp_pms_trans_code = 'N/A', 'N/A',
		    'N'
		) AS lkp_pms_trans_code_pre_month_end_is_close_trans,
		EXP_Default_Reserves_91_92.claimant_cov_det_ak_id,
		EXP_Default_Reserves_91_92.financial_type_code,
		EXP_Default_Reserves_91_92.pms_trans_code,
		EXP_Default_Reserves_91_92.trans_offset_onset_ind,
		LKP_claim_transaction_66_trans_for_92s.trans_date AS trans_date_pms_code_66,
		-- *INF*: IIF(isnull(trans_date_pms_code_66),TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),trans_date_pms_code_66)
		IFF(
		    trans_date_pms_code_66 IS NULL, TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
		    trans_date_pms_code_66
		) AS v_trans_date_pms_code_66,
		LKP_claim_transaction_66_trans_for_92s.pms_trans_code AS pms_trans_code_66,
		-- *INF*: IIF(ISNULL(pms_trans_code_66),'N/A',pms_trans_code_66)
		IFF(pms_trans_code_66 IS NULL, 'N/A', pms_trans_code_66) AS v_pms_trans_code_66,
		LKP_claim_transaction_41_42_22_post_month_end.trans_date AS trans_date_close_post_month_end,
		LKP_claim_transaction_41_42_22_post_month_end.pms_trans_code AS pms_trans_code_close_post_month_end,
		-- *INF*: IIF(isnull(pms_trans_code_close_post_month_end),'N/A',pms_trans_code_close_post_month_end)
		IFF(pms_trans_code_close_post_month_end IS NULL, 'N/A', pms_trans_code_close_post_month_end) AS v_pms_trans_code_close_post_month_end,
		EXP_Default_Reserves_91_92.LastDayOfLastMonth,
		-- *INF*: DECODE(true,
		-- pms_trans_code !='92','N/A',
		-- pms_trans_code='92' and v_pms_trans_code_66 !='N/A' and v_trans_date_pms_code_66 <= LastDayOfLastMonth,'92',
		-- pms_trans_code='92' and v_pms_trans_code_66 !='N/A' and v_trans_date_pms_code_66 > LastDayOfLastMonth and v_pms_trans_code_close_post_month_end !='N/A','90',
		-- '91'
		-- )
		-- 
		-- --  IF we have a 66 transaction that occurrs before end of month then we will revert to 92 -- because of inconsistancy with offsets onsets the search has to be done seperately
		-- -- IF we have a 66 after end of month and also a 41,42 or 22 close payment after end of month then revert to 90  - could also have been a 92.
		-- -- IF we have none of these scenarios and need to revert then go to a 91
		DECODE(
		    true,
		    pms_trans_code != '92', 'N/A',
		    pms_trans_code = '92' and v_pms_trans_code_66 != 'N/A' and v_trans_date_pms_code_66 <= LastDayOfLastMonth, '92',
		    pms_trans_code = '92' and v_pms_trans_code_66 != 'N/A' and v_trans_date_pms_code_66 > LastDayOfLastMonth and v_pms_trans_code_close_post_month_end != 'N/A', '90',
		    '91'
		) AS v_pms_trans_code_92_check,
		v_pms_trans_code_92_check AS pms_trans_code_92_check_out,
		-- *INF*: DECODE(true,
		-- v_pms_trans_code_close_post_month_end='N/A','N/A',
		-- pms_trans_code !='91','N/A',
		-- trans_offset_onset_ind !='N','N/A',
		-- v_pms_trans_code_close_post_month_end != 'N/A','90',
		-- '91'
		-- )
		-- 
		-- -- specific check where a 91 N is selected and a close transaction occurs post month end set an indicator to set it back to a 90
		-- -- else it should remain a 91.
		DECODE(
		    true,
		    v_pms_trans_code_close_post_month_end = 'N/A', 'N/A',
		    pms_trans_code != '91', 'N/A',
		    trans_offset_onset_ind != 'N', 'N/A',
		    v_pms_trans_code_close_post_month_end != 'N/A', '90',
		    '91'
		) AS v_pms_trans_code_91_N_check,
		v_pms_trans_code_91_N_check AS pms_trans_code_91_N_out
		FROM EXP_Default_Reserves_91_92
		LEFT JOIN LKP_claim_transaction_41_42_22_post_month_end
		ON LKP_claim_transaction_41_42_22_post_month_end.claimant_cov_det_ak_id = EXP_Default_Reserves_91_92.claimant_cov_det_ak_id AND LKP_claim_transaction_41_42_22_post_month_end.financial_type_code = EXP_Default_Reserves_91_92.financial_type_code AND LKP_claim_transaction_41_42_22_post_month_end.trans_date > EXP_Default_Reserves_91_92.LastDayOfLastMonth
		LEFT JOIN LKP_claim_transaction_41_42_66_22_pre_month_end
		ON LKP_claim_transaction_41_42_66_22_pre_month_end.claimant_cov_det_ak_id = EXP_Default_Reserves_91_92.claimant_cov_det_ak_id AND LKP_claim_transaction_41_42_66_22_pre_month_end.financial_type_code = EXP_Default_Reserves_91_92.financial_type_code AND LKP_claim_transaction_41_42_66_22_pre_month_end.trans_offset_onset_ind = EXP_Default_Reserves_91_92.trans_offset_onset_ind_for_lookup AND LKP_claim_transaction_41_42_66_22_pre_month_end.trans_date <= EXP_Default_Reserves_91_92.LastDayOfLastMonth
		LEFT JOIN LKP_claim_transaction_66_trans_for_92s
		ON LKP_claim_transaction_66_trans_for_92s.claimant_cov_det_ak_id = EXP_Default_Reserves_91_92.claimant_cov_det_ak_id AND LKP_claim_transaction_66_trans_for_92s.financial_type_code = EXP_Default_Reserves_91_92.financial_type_code
	),
	EXP_determine_conversion AS (
		SELECT
		claimant_cov_det_ak_id,
		financial_type_code,
		pms_trans_code,
		trans_offset_onset_ind,
		lkp_pms_trans_code_pre_month_end,
		lkp_pms_trans_code_pre_month_end_is_close_trans,
		pms_trans_code_92_check_out AS pms_trans_code_92_out,
		pms_trans_code_91_N_out,
		-- *INF*: DECODE( true,
		-- --92 is converted in prior step, if valid take the output
		-- pms_trans_code='92' and 
		-- pms_trans_code_92_out !='N/A',
		-- pms_trans_code_92_out,
		-- 
		-- -- if a 91onset comes in and there is a 22/41/42 offset and it is != NA from the rules in the prior expression use that output value
		-- pms_trans_code='91' and 
		-- trans_offset_onset_ind='N' and 
		-- lkp_pms_trans_code_pre_month_end_is_close_trans='Y' and 
		-- pms_trans_code_91_N_out !='N/A',
		-- pms_trans_code_91_N_out,
		-- 
		-- -- if 91 onset and no offsets then we want to revert to the offsets value as if no onset/offset happened.
		-- pms_trans_code='91' and 
		-- trans_offset_onset_ind='N' and 
		-- lkp_pms_trans_code_pre_month_end_is_close_trans !='Y',
		-- pms_trans_code,
		-- 
		-- -- if 91 and there is a 22/41/42 then keep it as a 91.  This is a non-offset/onset situation
		-- pms_trans_code='91' and 
		-- lkp_pms_trans_code_pre_month_end_is_close_trans='Y' , 
		-- '91',
		-- 
		-- -- if 91 and no 41/42 then at month end this was a 90.  This is a non-offset/onset situation
		-- pms_trans_code='91' and 
		-- lkp_pms_trans_code_pre_month_end_is_close_trans != 'Y' , 
		-- '90',
		-- 
		-- -- if 90 onset,the SQ only reads onset 90s in, and offset 22/41/42 was found then this is ok.
		-- pms_trans_code='90' and 
		-- lkp_pms_trans_code_pre_month_end_is_close_trans='Y', 
		-- pms_trans_code, 
		-- 
		-- -- if 90 onset, the SQ only reads onset 90s in, and offset 22/41/42 was not found then this needs to be set to 91 so it gets filtered, else we get double reserves
		-- pms_trans_code='90' and 
		-- lkp_pms_trans_code_pre_month_end='N/A',
		-- '91',
		-- 
		-- --default if none of these scenarios fit.
		-- pms_trans_code)
		-- 
		-- 
		DECODE(
		    true,
		    pms_trans_code = '92' and pms_trans_code_92_out != 'N/A', pms_trans_code_92_out,
		    pms_trans_code = '91' and trans_offset_onset_ind = 'N' and lkp_pms_trans_code_pre_month_end_is_close_trans = 'Y' and pms_trans_code_91_N_out != 'N/A', pms_trans_code_91_N_out,
		    pms_trans_code = '91' and trans_offset_onset_ind = 'N' and lkp_pms_trans_code_pre_month_end_is_close_trans != 'Y', pms_trans_code,
		    pms_trans_code = '91' and lkp_pms_trans_code_pre_month_end_is_close_trans = 'Y', '91',
		    pms_trans_code = '91' and lkp_pms_trans_code_pre_month_end_is_close_trans != 'Y', '90',
		    pms_trans_code = '90' and lkp_pms_trans_code_pre_month_end_is_close_trans = 'Y', pms_trans_code,
		    pms_trans_code = '90' and lkp_pms_trans_code_pre_month_end = 'N/A', '91',
		    pms_trans_code
		) AS v_pms_trans_code,
		v_pms_trans_code AS pms_trans_code_OUT
		FROM EXP_check_transcode_lkp_output
	),
	OUTPUT_LM_90N_91_92_conversion AS (
		SELECT
		pms_trans_code_OUT
		FROM EXP_determine_conversion
	),
),
EXP_conversion_output AS (
	SELECT
	EXP_Default_Reserves_91_92.claim_trans_id,
	EXP_Default_Reserves_91_92.claim_trans_ak_id,
	EXP_Default_Reserves_91_92.claimant_cov_det_ak_id,
	EXP_Default_Reserves_91_92.claim_pay_ak_id,
	EXP_Default_Reserves_91_92.cause_of_loss,
	EXP_Default_Reserves_91_92.reserve_ctgry,
	EXP_Default_Reserves_91_92.type_disability,
	EXP_Default_Reserves_91_92.sar_id,
	EXP_Default_Reserves_91_92.financial_type_code,
	EXP_Default_Reserves_91_92.s3p_trans_code,
	EXP_Default_Reserves_91_92.pms_trans_code,
	EXP_Default_Reserves_91_92.trans_code,
	EXP_Default_Reserves_91_92.trans_date,
	EXP_Default_Reserves_91_92.pms_acct_entered_date,
	EXP_Default_Reserves_91_92.trans_base_type_code,
	EXP_Default_Reserves_91_92.trans_ctgry_code,
	EXP_Default_Reserves_91_92.Exceed_Trans_Code,
	EXP_Default_Reserves_91_92.trans_amt,
	EXP_Default_Reserves_91_92.trans_hist_amt,
	EXP_Default_Reserves_91_92.draft_num,
	EXP_Default_Reserves_91_92.source_sys_id,
	EXP_Default_Reserves_91_92.trans_offset_onset_ind,
	mplt_loss_master_90N_91_92_pms_trans_code_conversions.pms_trans_code_OUT AS mplt_pms_trans_code,
	EXP_Default_Reserves_91_92.LastDayOfLastMonth
	FROM EXP_Default_Reserves_91_92
	 -- Manually join with mplt_loss_master_90N_91_92_pms_trans_code_conversions
),
Union AS (
	SELECT claim_trans_id, claim_trans_ak_id, claimant_cov_det_ak_id, claim_pay_ak_id, cause_of_loss, reserve_ctgry, type_disability, sar_id, financial_type_code, s3p_trans_code, pms_trans_code, trans_code, trans_date, pms_acct_entered_date, trans_base_type_code, trans_ctgry_code, Exceed_Trans_Code, trans_amt, trans_hist_amt, draft_num, source_sys_id, trans_offset_onset_ind, LastDayOfLastMonth
	FROM EXP_Default_Reserves
	UNION
	SELECT claim_trans_id, claim_trans_ak_id, claimant_cov_det_ak_id, claim_pay_ak_id, cause_of_loss, reserve_ctgry, type_disability, sar_id, financial_type_code, s3p_trans_code, mplt_pms_trans_code AS pms_trans_code, trans_code, trans_date, pms_acct_entered_date, trans_base_type_code, trans_ctgry_code, Exceed_Trans_Code, trans_amt, trans_hist_amt, draft_num, source_sys_id, trans_offset_onset_ind, LastDayOfLastMonth
	FROM EXP_conversion_output
),
EXP_union_output AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	financial_type_code,
	s3p_trans_code,
	pms_trans_code,
	trans_code,
	trans_date,
	pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	Exceed_Trans_Code,
	trans_amt,
	trans_hist_amt,
	draft_num,
	source_sys_id,
	trans_offset_onset_ind,
	LastDayOfLastMonth AS sql_date_out
	FROM Union
),
EXP_Loss_Master_Offset_Onset_Reserves AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	financial_type_code,
	s3p_trans_code,
	pms_trans_code,
	trans_code,
	trans_date,
	pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	Exceed_Trans_Code,
	trans_amt,
	trans_hist_amt,
	draft_num,
	source_sys_id,
	trans_offset_onset_ind,
	-- *INF*: ADD_TO_DATE(SYSDATE,'MM',-1)
	DATEADD(MONTH,- 1,CURRENT_TIMESTAMP) AS V_Last_Month_Date,
	-- *INF*: LAST_DAY(V_Last_Month_Date)
	LAST_DAY(V_Last_Month_Date) AS V_Last_Month_Last_Day_Date,
	-- *INF*: GET_DATE_PART(V_Last_Month_Last_Day_Date,'YYYY')
	DATE_PART(V_Last_Month_Last_Day_Date, 'YYYY') AS V_Account_Date_YYYY,
	-- *INF*: GET_DATE_PART(V_Last_Month_Last_Day_Date,'MM')
	DATE_PART(V_Last_Month_Last_Day_Date, 'MM') AS V_Account_Date_MM,
	-- *INF*: V_Account_Date_YYYY || LPAD(V_Account_Date_MM,2,'0')
	V_Account_Date_YYYY || LPAD(V_Account_Date_MM, 2, '0') AS V_Account_Date,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART( V_Last_Month_Last_Day_Date, 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	-- 
	-- 
	-- --- Changing the date to have 00:00:00 in the timestamp part
	DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date)),DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date)),DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date))) AS v_Loss_Master_Run_Date,
	-- *INF*: IIF(trans_offset_onset_ind = 'O', pms_acct_entered_date, ADD_TO_DATE(v_Loss_Master_Run_Date,'dd',1))
	-- 
	-- --- Above logic is very important for Loss Master generation for EDW. We had to use above so that for EXCEED Offset Transactions we can get the attributes from Dim tables as that day so we are using pms_acct_entered_date. And for other transactions we use loss_master_run_date.
	IFF(
	    trans_offset_onset_ind = 'O', pms_acct_entered_date, DATEADD(dd,1,v_Loss_Master_Run_Date)
	) AS Loss_Master_Run_Date
	FROM EXP_union_output
),
LKP_claim_transaction_first_code AS (
	SELECT
	trans_date,
	s3p_trans_code,
	financial_type_code,
	claimant_cov_det_ak_id,
	source_sys_id,
	trans_offset_onset_ind,
	IN_claimant_cov_det_ak_id,
	IN_financial_type_code,
	IN_source_sys_id,
	IN_trans_offset_onset_ind
	FROM (
		SELECT 
		CT.trans_date as trans_date, 
		CT.s3p_trans_code as s3p_trans_code, 
		CT.trans_offset_onset_ind as trans_offset_onset_ind, 
		CT.claimant_cov_det_ak_id as claimant_cov_det_ak_id, 
		CT.financial_type_code as financial_type_code, 
		CT.source_sys_id as source_sys_id 
		FROM vw_claim_transaction CT
		where
		CT.trans_offset_onset_ind <> 'O'
		order by CT.trans_date desc --
		
		-- known informatica bug when using order by asc will actually order by desc, so ordering by desc and trying a workaround
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,financial_type_code,source_sys_id ORDER BY trans_date DESC) = 1
),
SQL_calculate_reserves AS (-- SQL_calculate_reserves

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
mplt_LM_Policy_n_Claim_Attributes_Reserves AS (WITH
	INPUT AS (
		
	),
	LKP_Claimant_Coverage_Detail AS (
		SELECT
		claim_party_occurrence_ak_id,
		loc_unit_num,
		sub_loc_unit_num,
		ins_line,
		risk_unit_grp,
		risk_unit_grp_seq_num,
		risk_unit,
		risk_unit_seq_num,
		major_peril_code,
		major_peril_seq,
		pms_loss_disability,
		reserve_ctgry,
		cause_of_loss,
		pms_type_exposure,
		pms_type_bureau_code,
		risk_type_ind,
		PolicySourceID,
		ClassCode,
		SublineCode,
		RatingCoverageAKId,
		StatisticalCoverageAKID,
		PolicyCoverageAKID,
		RiskLocationAKID,
		PremiumTransactionAKID,
		BureauStatisticalCodeAKID,
		RiskTerritory,
		StateProvinceCode,
		ZipPostalCode,
		TaxLocation,
		Exposure,
		BureauStatisticalcode1_15,
		BureauSpecialUseCode,
		PMSAnnualStatementLine,
		RatingDateIndicator,
		BureauStatisticalUserLine,
		AuditReinstatementIndicator,
		claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT Ab.claim_party_occurrence_ak_id AS Claim_party_occurrence_ak_id,
			       Ab.loc_unit_num                 AS Loc_unit_num,
			       Ab.sub_loc_unit_num             AS Sub_loc_unit_num,
			       Ab.ins_line                     AS Ins_line,
			       Ab.risk_unit_grp                AS Risk_unit_grp,
			       Ab.risk_unit_grp_seq_num        AS Risk_unit_grp_seq_num,
			       Ab.risk_unit                    AS Risk_unit,
			       Ab.risk_unit_seq_num            AS Risk_unit_seq_num,
			       Ab.major_peril_code             AS Major_peril_code,
			       Ab.major_peril_seq              AS Major_peril_seq,
			       Ab.pms_loss_disability          AS Pms_loss_disability,
			       Ab.reserve_ctgry                AS Reserve_ctgry,
			       Ab.cause_of_loss                AS Cause_of_loss,
			       Ab.pms_type_exposure            AS Pms_type_exposure,
			       Ab.pms_type_bureau_code         AS Pms_type_bureau_code,
			       Ab.risk_type_ind                AS Risk_type_ind,
			       Ab.PolicySourceID               AS Policysourceid,
			       isnull(Sc.ClassCode,'N/A')                    AS Classcode,
			       isnull(Sc.SublineCode,'N/A')                  AS Sublinecode,
			       Ab.RatingCoverageAKId           AS Ratingcoverageakid,
			       Ab.StatisticalCoverageAKID      AS Statisticalcoverageakid,
			       isnull(Pc.PolicyCoverageAKID,-1)           AS Policycoverageakid,
			       isnull(Rl.RiskLocationAKID,-1)             AS Risklocationakid,
			       isnull(Pt.PremiumTransactionAKID,-1)       AS Premiumtransactionakid,
			       isnull(Bsc.BureauStatisticalCodeAKID,-1)   AS Bureaustatisticalcodeakid,
			       isnull(Rl.RiskTerritory, 'N/A')              AS Riskterritory,
			       isnull(Rl.StateProvinceCode , 'N/A')           AS Stateprovincecode,
			       isnull(Rl.ZipPostalCode,'N/A')                AS Zippostalcode,
			       isnull(Rl.TaxLocation,'N/A')                  AS Taxlocation,
			       isnull(Pt.Exposure,0)                     AS Exposure,
			       isnull(BureauCode1 + BureauCode2 + BureauCode3 + BureauCode4 + BureauCode5 + 
			       BureauCode6 + BureauCode7 + BureauCode8 + BureauCode9 + BureauCode10 + 
			       BureauCode11 + BureauCode12 + BureauCode13 + BureauCode14 + BureauCode15, 'N/A') AS Bureaustatisticalcode1_15,
			       isnull(BureauSpecialUseCode, 'N/A')            AS BureauSpecialUseCode,
			       isnull(PMSAnnualStatementLine, 'N/A')		   AS PMSAnnualStatementLine,
			       isnull(RatingDateIndicator, 'N/A')             AS RatingDateIndicator,
			       isnull(BureauStatisticalUserLine, 'N/A')       AS BureauStatisticalUserLine,
			       isnull(AuditReinstatementIndicator, 'N/A')     AS AuditReinstatementIndicator,
			       Ab.claimant_cov_det_ak_id       AS Claimant_cov_det_ak_id,
			       Ab.eff_from_date                AS Eff_from_date,
			       Ab.eff_to_date                  AS Eff_to_date
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL Ab
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.STATISTICALCOVERAGE Sc
			on Ab.StatisticalCoverageAKID = Sc.StatisticalCoverageAKID
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.POLICYCOVERAGE Pc
			on Sc.PolicyCoverageAKID = Pc.PolicyCoverageAKID
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.RISKLOCATION Rl
			on Pc.RiskLocationAKID = Rl.RiskLocationAKID
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.PREMIUMTRANSACTION Pt
			on Sc.StatisticalCoverageAKID = Pt.StatisticalCoverageAKID
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.BUREAUSTATISTICALCODE Bsc
			on Pt.PremiumTransactionAKID = Bsc.PremiumTransactionAKID
			WHERE -- Ab.audit_id > 0 AND -- (Removing the Audit_ID condition as part of Ticket #663312)
				  Ab.PolicySourceID NOT IN ( 'PDC', 'DUC' )  ---- To pull data from coverages of PMS policies only
			       
			UNION
			
			SELECT Ab.claim_party_occurrence_ak_id AS Claim_party_occurrence_ak_id,
			       Ab.loc_unit_num                 AS Loc_unit_num,
			       Ab.sub_loc_unit_num             AS Sub_loc_unit_num,
			       Ab.ins_line                     AS Ins_line,
			       Ab.risk_unit_grp                AS Risk_unit_grp,
			       Ab.risk_unit_grp_seq_num        AS Risk_unit_grp_seq_num,
			       Ab.risk_unit                    AS Risk_unit,
			       Ab.risk_unit_seq_num            AS Risk_unit_seq_num,
			       Ab.major_peril_code             AS Major_peril_code,
			       Ab.major_peril_seq              AS Major_peril_seq,
			       Ab.pms_loss_disability          AS Pms_loss_disability,
			       Ab.reserve_ctgry                AS Reserve_ctgry,
			       Ab.cause_of_loss                AS Cause_of_loss,
			       Ab.pms_type_exposure            AS Pms_type_exposure,
			       Ab.pms_type_bureau_code         AS Pms_type_bureau_code,
			       Ab.risk_type_ind                AS Risk_type_ind,
			       Ab.PolicySourceID               AS Policysourceid,
			       Rc.Classcode                  AS Classcode,
			       Ab.SublineCode                  AS Sublinecode,
			       Ab.RatingCoverageAKId           AS Ratingcoverageakid,
			       Ab.StatisticalCoverageAKID      AS Statisticalcoverageakid,
			       isnull(Pc.PolicyCoverageAKID,-1)           AS Policycoverageakid,
			       isnull(Rl.RiskLocationAKID,-1)             AS Risklocationakid,
			       isnull(Pt.PremiumTransactionAKID,-1)       AS Premiumtransactionakid,
			       -1                              AS Bureaustatisticalcodeakid,
			       isnull(Rl.RiskTerritory, 'N/A')                AS Riskterritory,
			       isnull(Rl.StateProvinceCode, 'N/A')            AS Stateprovincecode,
			       isnull(Rl.ZipPostalCode, 'N/A')                AS Zippostalcode,
			       isnull(Rl.TaxLocation,' N/A')                  AS Taxlocation,
			       isnull(Rc.Exposure, 0)                     AS Exposure,
			       'N/A'                           AS Bureaustatisticalcode1_15,
			       'N/A'                           AS BureauSpecialUseCode,
			       'N/A'                           AS PMSAnnualStatementLine,
			       'N/A'                           AS RatingDateIndicator,
			       'N/A'                           AS BureauStatisticalUserLine,
			       'N/A'                           AS AuditReinstatementIndicator,
			       Ab.claimant_cov_det_ak_id       AS Claimant_cov_det_ak_id,
			       Ab.eff_from_date                AS Eff_from_date,
			       Ab.eff_to_date                  AS Eff_to_date
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL Ab
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage Rc
			on Ab.RatingCoverageAKId = Rc.RatingCoverageAKID 
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.POLICYCOVERAGE Pc
			on Rc.PolicyCoverageAKID = Pc.PolicyCoverageAKID 
			and Pc.CurrentSnapshotFlag=1
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.RISKLOCATION Rl
			on Pc.RiskLocationAKID = Rl.RiskLocationAKID 
			and Rl.CurrentSnapshotFlag=1
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.PREMIUMTRANSACTION Pt
			on Rc.RatingCoverageAKID = Pt.RatingCoverageAKID 
			and Pt.EffectiveDate=Rc.EffectiveDate
			and Pt.CurrentSnapshotFlag=1
			WHERE  -- Ab.audit_id > 0 AND -- -- (Removing the Audit_ID condition as part of Ticket #663312)
				   Ab.PolicySourceID IN ( 'PDC', 'DUC' )  ---- To pull data for coverages of DuckCreek policies only
			ORDER BY Ab.claimant_cov_det_ak_id--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claim_party_occurrence_ak_id DESC) = 1
	),
	LKP_Claim_Paymeny_Category AS (
		SELECT
		cost_containment_saving_amt,
		claim_pay_ak_id
		FROM (
			SELECT claim_payment_category.cost_containment_saving_amt as cost_containment_saving_amt, claim_payment_category.claim_pay_ak_id as claim_pay_ak_id 
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_payment_category
			WHERE claim_pay_ctgry_type = 'CC'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id ORDER BY cost_containment_saving_amt DESC) = 1
	),
	LKP_Claim_Party_Occurrence AS (
		SELECT
		claim_party_occurrence_ak_id,
		claim_occurrence_ak_id,
		claim_party_ak_id,
		claim_case_ak_id,
		claim_occurrence_key,
		source_claim_occurrence_status_code,
		policy_key,
		policy_ak_id,
		Exceed_Claim_Number,
		claimant_num,
		claim_loss_date,
		claim_occurrence_num
		FROM (
			SELECT CPO.claim_occurrence_ak_id             AS claim_occurrence_ak_id,
			       CP.claim_party_ak_id                   AS claim_party_ak_id,
			       CPO.claim_case_ak_id 	           AS claim_case_ak_id,
			       CO.claim_occurrence_key                AS claim_occurrence_key,
			       CO.source_claim_occurrence_status_code AS source_claim_occurrence_status_code,
			       CO.pol_key                             AS policy_key,
			       CO.pol_key_ak_id                    AS policy_ak_id, 
			       CO.s3p_claim_num                       AS Exceed_Claim_Number,
			       CPO.claimant_num                       AS claimant_num,
			       CO.claim_loss_date                     AS claim_loss_date,
			       CO.claim_occurrence_num				  AS claim_occurrence_num,
			       CPO.claim_party_occurrence_ak_id       AS claim_party_occurrence_ak_id
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
			       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO,
			       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP
			WHERE  CPO.claim_occurrence_ak_id = CO.claim_occurrence_ak_id
			       AND CP.claim_party_ak_id = CPO.claim_party_ak_id
			       AND CO.crrnt_snpsht_flag = 1
			       AND CPO.crrnt_snpsht_flag = 1
			       AND CP.crrnt_snpsht_flag = 1
			       AND CPO.claim_party_role_code IN ( 'CLMT', 'CMT' )
			 ORDER  BY claimant_num, claim_party_role_code --
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY claim_party_occurrence_ak_id DESC) = 1
	),
	LKP_Workers_Comp_Claimant_Detail AS (
		SELECT
		wc_claimant_det_ak_id,
		claim_party_occurrence_ak_id
		FROM (
			SELECT WCCD.wc_claimant_det_ak_id        AS wc_claimant_det_ak_id,
			       WCCD.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_detail  WCCD
			WHERE WCCD.crrnt_snpsht_flag =1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY wc_claimant_det_ak_id DESC) = 1
	),
	LKP_Claim_Case AS (
		SELECT
		claim_case_ak_id,
		prim_litigation_handler_ak_id
		FROM (
			SELECT CC.prim_litigation_handler_ak_id     AS prim_litigation_handler_ak_id,
			       CC.suit_status_code                  AS suit_status_code,
			       CC.prim_litigation_handler_role_code AS prim_litigation_handler_role_code,
			       CC.suit_open_date                    AS suit_open_date,
			       CC.suit_close_date                   AS suit_close_date,
			       CC.claim_case_ak_id                  AS claim_case_ak_id
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case CC
			WHERE CC.crrnt_snpsht_flag =1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_ak_id ORDER BY claim_case_ak_id DESC) = 1
	),
	Claim_Representative_H AS (
		SELECT
		claim_rep_ak_id,
		claim_occurrence_ak_id
		FROM (
			SELECT CR.claim_rep_ak_id         AS claim_rep_ak_id,
			          CRO.claim_occurrence_ak_id AS claim_occurrence_ak_id
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative CR,
			       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_occurrence CRO
			WHERE  CR.claim_rep_ak_id = CRO.claim_rep_ak_id
			       AND CR.crrnt_snpsht_flag = 1
			       AND CRO.crrnt_snpsht_flag = 1
			       AND CRO.claim_rep_role_code = 'H'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id ORDER BY claim_rep_ak_id) = 1
	),
	Claim_Representative_Examiner AS (
		SELECT
		claim_rep_ak_id,
		claim_occurrence_ak_id
		FROM (
			SELECT CR.claim_rep_ak_id         AS claim_rep_ak_id,
			       CRO.claim_occurrence_ak_id AS claim_occurrence_ak_id
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative CR,
			       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_occurrence CRO
			WHERE  CR.claim_rep_ak_id = CRO.claim_rep_ak_id
			       AND CR.crrnt_snpsht_flag = 1
			       AND CRO.crrnt_snpsht_flag = 1
			       AND CRO.claim_rep_role_code = 'E'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_Policy_Contract_Customer AS (
		SELECT
		pol_ak_id,
		contract_cust_ak_id,
		agency_ak_id,
		pol_key,
		mco,
		pol_eff_date,
		pol_exp_date,
		pms_pol_lob_code,
		ClassOfBusiness,
		variation_code,
		IN_policy_ak_id
		FROM (
			SELECT       P.contract_cust_ak_id AS contract_cust_ak_id,
			       P.AgencyAKID      AS agency_ak_id,
			       P.pol_key             AS pol_key,
			       P.mco                    AS mco,
			       P.pol_eff_date        AS pol_eff_date,
			       P.pol_exp_date        AS pol_exp_date,
			       P.pms_pol_lob_code    AS pms_pol_lob_code,
			       P.ClassOfBusiness as ClassOfBusiness,
			       P.variation_code      AS variation_code,
			       P.pol_ak_id           AS pol_ak_id
			FROM   
			       @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer CC,
			       @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy P, 
			       @{pipeline().parameters.SOURCE_TABLE_OWNER}.agency A
			WHERE  CC.contract_cust_ak_id = P.contract_cust_ak_id
			       AND P.AgencyAKId  = A.AgencyAKId
			       AND CC.crrnt_snpsht_flag = 1
			       AND P.crrnt_snpsht_flag = 1
			       AND A.CurrentSnapshotFlag =1
			       AND P.pol_ak_id  IN (SELECT DISTINCT pol_key_ak_id FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence)
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id ORDER BY pol_ak_id DESC) = 1
	),
	EXP_Values AS (
		SELECT
		LKP_Policy_Contract_Customer.IN_policy_ak_id AS pol_ak_id,
		LKP_Policy_Contract_Customer.contract_cust_ak_id,
		LKP_Policy_Contract_Customer.agency_ak_id,
		LKP_Policy_Contract_Customer.pol_key,
		LKP_Policy_Contract_Customer.mco,
		LKP_Policy_Contract_Customer.pol_eff_date,
		LKP_Policy_Contract_Customer.pol_exp_date,
		LKP_Policy_Contract_Customer.pms_pol_lob_code,
		LKP_Policy_Contract_Customer.ClassOfBusiness,
		LKP_Policy_Contract_Customer.variation_code,
		LKP_Claim_Party_Occurrence.claim_party_occurrence_ak_id,
		LKP_Claim_Party_Occurrence.claim_occurrence_ak_id,
		LKP_Claim_Party_Occurrence.claim_party_ak_id,
		Claim_Representative_H.claim_rep_ak_id AS claim_rep_ak_id_H,
		Claim_Representative_Examiner.claim_rep_ak_id AS claim_rep_ak_id_E,
		LKP_Claim_Case.claim_case_ak_id,
		LKP_Workers_Comp_Claimant_Detail.wc_claimant_det_ak_id,
		LKP_Claim_Case.prim_litigation_handler_ak_id,
		LKP_Claim_Party_Occurrence.claim_occurrence_key,
		LKP_Claim_Party_Occurrence.source_claim_occurrence_status_code,
		LKP_Claim_Party_Occurrence.policy_key,
		LKP_Claim_Party_Occurrence.Exceed_Claim_Number,
		LKP_Claim_Party_Occurrence.claimant_num,
		LKP_Claim_Party_Occurrence.claim_loss_date,
		LKP_Claim_Party_Occurrence.claim_occurrence_num,
		LKP_Claimant_Coverage_Detail.loc_unit_num,
		LKP_Claimant_Coverage_Detail.sub_loc_unit_num,
		LKP_Claimant_Coverage_Detail.ins_line,
		LKP_Claimant_Coverage_Detail.risk_unit_grp,
		LKP_Claimant_Coverage_Detail.risk_unit_grp_seq_num,
		LKP_Claimant_Coverage_Detail.risk_unit,
		LKP_Claimant_Coverage_Detail.risk_unit_seq_num,
		LKP_Claimant_Coverage_Detail.risk_type_ind,
		LKP_Claimant_Coverage_Detail.major_peril_code,
		LKP_Claimant_Coverage_Detail.major_peril_seq,
		LKP_Claimant_Coverage_Detail.pms_loss_disability,
		LKP_Claimant_Coverage_Detail.reserve_ctgry,
		LKP_Claimant_Coverage_Detail.cause_of_loss,
		LKP_Claimant_Coverage_Detail.pms_type_exposure,
		LKP_Claimant_Coverage_Detail.pms_type_bureau_code,
		LKP_Claim_Paymeny_Category.cost_containment_saving_amt,
		LKP_Claimant_Coverage_Detail.PolicySourceID,
		LKP_Claimant_Coverage_Detail.ClassCode,
		LKP_Claimant_Coverage_Detail.SublineCode,
		LKP_Claimant_Coverage_Detail.RatingCoverageAKId,
		LKP_Claimant_Coverage_Detail.StatisticalCoverageAKID,
		LKP_Claimant_Coverage_Detail.PolicyCoverageAKID,
		LKP_Claimant_Coverage_Detail.RiskLocationAKID,
		LKP_Claimant_Coverage_Detail.PremiumTransactionAKID,
		LKP_Claimant_Coverage_Detail.BureauStatisticalCodeAKID,
		LKP_Claimant_Coverage_Detail.RiskTerritory,
		LKP_Claimant_Coverage_Detail.StateProvinceCode,
		LKP_Claimant_Coverage_Detail.ZipPostalCode,
		LKP_Claimant_Coverage_Detail.TaxLocation,
		LKP_Claimant_Coverage_Detail.Exposure,
		LKP_Claimant_Coverage_Detail.BureauStatisticalcode1_15,
		LKP_Claimant_Coverage_Detail.BureauSpecialUseCode,
		LKP_Claimant_Coverage_Detail.PMSAnnualStatementLine,
		LKP_Claimant_Coverage_Detail.RatingDateIndicator,
		LKP_Claimant_Coverage_Detail.BureauStatisticalUserLine,
		LKP_Claimant_Coverage_Detail.AuditReinstatementIndicator
		FROM 
		LEFT JOIN Claim_Representative_Examiner
		ON Claim_Representative_Examiner.claim_occurrence_ak_id = LKP_Claim_Party_Occurrence.claim_occurrence_ak_id
		LEFT JOIN Claim_Representative_H
		ON Claim_Representative_H.claim_occurrence_ak_id = LKP_Claim_Party_Occurrence.claim_occurrence_ak_id
		LEFT JOIN LKP_Claim_Case
		ON LKP_Claim_Case.claim_case_ak_id = LKP_Claim_Party_Occurrence.claim_case_ak_id
		LEFT JOIN LKP_Claim_Party_Occurrence
		ON LKP_Claim_Party_Occurrence.claim_party_occurrence_ak_id = LKP_Claimant_Coverage_Detail.claim_party_occurrence_ak_id
		LEFT JOIN LKP_Claim_Paymeny_Category
		ON LKP_Claim_Paymeny_Category.claim_pay_ak_id = INPUT.IN_claim_pay_ak_id
		LEFT JOIN LKP_Claimant_Coverage_Detail
		ON LKP_Claimant_Coverage_Detail.claimant_cov_det_ak_id = INPUT.IN_claimant_cov_det_ak_id AND LKP_Claimant_Coverage_Detail.eff_from_date <= INPUT.IN_Date AND LKP_Claimant_Coverage_Detail.eff_to_date >= INPUT.IN_Date
		LEFT JOIN LKP_Policy_Contract_Customer
		ON LKP_Policy_Contract_Customer.pol_ak_id = LKP_Claim_Party_Occurrence.policy_ak_id
		LEFT JOIN LKP_Workers_Comp_Claimant_Detail
		ON LKP_Workers_Comp_Claimant_Detail.claim_party_occurrence_ak_id = LKP_Claimant_Coverage_Detail.claim_party_occurrence_ak_id
	),
	OUTPUT AS (
		SELECT
		pol_ak_id, 
		contract_cust_ak_id, 
		agency_ak_id, 
		pol_key, 
		mco, 
		pol_eff_date, 
		pol_exp_date, 
		pms_pol_lob_code, 
		ClassOfBusiness, 
		variation_code, 
		claim_party_occurrence_ak_id, 
		claim_occurrence_ak_id, 
		claim_party_ak_id, 
		claim_rep_ak_id_H, 
		claim_rep_ak_id_E, 
		claim_case_ak_id, 
		wc_claimant_det_ak_id, 
		claim_occurrence_key, 
		source_claim_occurrence_status_code, 
		policy_key, 
		Exceed_Claim_Number, 
		claimant_num, 
		claim_loss_date, 
		claim_occurrence_num, 
		loc_unit_num, 
		sub_loc_unit_num, 
		ins_line, 
		risk_unit_grp, 
		risk_unit_grp_seq_num, 
		risk_unit, 
		risk_unit_seq_num, 
		risk_type_ind, 
		major_peril_code, 
		major_peril_seq, 
		pms_loss_disability, 
		reserve_ctgry, 
		cause_of_loss, 
		pms_type_exposure, 
		pms_type_bureau_code, 
		cost_containment_saving_amt, 
		PolicySourceID, 
		ClassCode, 
		SublineCode, 
		RatingCoverageAKId, 
		StatisticalCoverageAKID, 
		PolicyCoverageAKID, 
		RiskLocationAKID, 
		PremiumTransactionAKID, 
		BureauStatisticalCodeAKID, 
		RiskTerritory, 
		StateProvinceCode, 
		ZipPostalCode, 
		TaxLocation, 
		Exposure, 
		BureauStatisticalcode1_15, 
		BureauSpecialUseCode, 
		PMSAnnualStatementLine, 
		RatingDateIndicator, 
		BureauStatisticalUserLine, 
		AuditReinstatementIndicator
		FROM EXP_Values
	),
),
EXP_Determine_Loss_Master_Output_Rows_Reserves AS (
	SELECT
	EXP_Loss_Master_Offset_Onset_Reserves.claim_trans_id,
	EXP_Loss_Master_Offset_Onset_Reserves.claim_trans_ak_id,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.claim_party_occurrence_ak_id,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.claim_party_ak_id,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.claim_occurrence_ak_id,
	EXP_Loss_Master_Offset_Onset_Reserves.claimant_cov_det_ak_id,
	EXP_Loss_Master_Offset_Onset_Reserves.claim_pay_ak_id,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.pol_ak_id,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.contract_cust_ak_id,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.agency_ak_id,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.pol_eff_date,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.pol_exp_date,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.pms_pol_lob_code,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.variation_code,
	-- *INF*: IIF(IN(pms_pol_lob_code,'ACA','AFA','APA','ATA','ACJ','AFJ','APJ'),'6',variation_code)
	IFF(pms_pol_lob_code IN ('ACA','AFA','APA','ATA','ACJ','AFJ','APJ'), '6', variation_code) AS LM_Variation_Code,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.claim_rep_ak_id_H,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.claim_rep_ak_id_E,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.claim_case_ak_id,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.wc_claimant_det_ak_id,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.claim_occurrence_key,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.source_claim_occurrence_status_code,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.policy_key AS Policy_key,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.Exceed_Claim_Number AS s3p_Claim_Num,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.claimant_num,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.claim_loss_date AS Claim_loss_date,
	-- *INF*: SUBSTR(Policy_key,1,3)
	SUBSTR(Policy_key, 1, 3) AS V_Policy_Symbol,
	V_Policy_Symbol AS Policy_Symbol,
	-- *INF*: SUBSTR(Policy_key,4,7)
	SUBSTR(Policy_key, 4, 7) AS V_Policy_Number,
	V_Policy_Number AS Policy_Number,
	-- *INF*: SUBSTR(Policy_key,11,2)
	SUBSTR(Policy_key, 11, 2) AS V_Policy_Module,
	V_Policy_Module AS Policy_Module,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.loc_unit_num,
	-- *INF*: IIF(loc_unit_num = 'N/A','0000',loc_unit_num)
	-- 
	-- 
	-- --Adding new rules for Personal Lines policy as the coverage EDW is incorrect.
	-- ----IIF(loc_unit_num = 'N/A','0000',loc_unit_num)
	-- 
	-- -----IIF(loc_unit_num = 'N/A','0000',
	--     --   IIF(SUBSTR(Policy_key,1,1)='H' and SUBSTR(Policy_key,4,1) = '5','0000',loc_unit_num)
	--        ---)
	IFF(loc_unit_num = 'N/A', '0000', loc_unit_num) AS v_loc_unit_num,
	v_loc_unit_num AS loc_unit_num_out,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.sub_loc_unit_num,
	-- *INF*: IIF(sub_loc_unit_num='N/A','000',sub_loc_unit_num)
	IFF(sub_loc_unit_num = 'N/A', '000', sub_loc_unit_num) AS v_sub_loc_unit_num,
	v_sub_loc_unit_num AS sub_loc_unit_num_out,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.ins_line,
	-- *INF*: IIF(ins_line = 'N/A','NA',ins_line)
	IFF(ins_line = 'N/A', 'NA', ins_line) AS ins_line_out,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.risk_unit_grp,
	-- *INF*: IIF(risk_unit_grp = 'N/A','000',risk_unit_grp)
	IFF(risk_unit_grp = 'N/A', '000', risk_unit_grp) AS risk_unit_grp_out,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.risk_unit_grp_seq_num,
	-- *INF*: IIF(LENGTH(RTRIM(risk_unit_grp_seq_num))<3,LPAD(RTRIM(risk_unit_grp_seq_num),3,'0'),risk_unit_grp_seq_num)
	IFF(
	    LENGTH(RTRIM(risk_unit_grp_seq_num)) < 3, LPAD(RTRIM(risk_unit_grp_seq_num), 3, '0'),
	    risk_unit_grp_seq_num
	) AS v_risk_unit_grp_seq_num,
	-- *INF*: IIF(SUBSTR(v_risk_unit_grp_seq_num,1,2)='N/','NA',SUBSTR(v_risk_unit_grp_seq_num,1,2))
	IFF(
	    SUBSTR(v_risk_unit_grp_seq_num, 1, 2) = 'N/', 'NA', SUBSTR(v_risk_unit_grp_seq_num, 1, 2)
	) AS risk_unit_grp_seq_num_First_2pos,
	-- *INF*: IIF(SUBSTR(v_risk_unit_grp_seq_num,3,1)='A','N',SUBSTR(v_risk_unit_grp_seq_num,3,1))
	IFF(SUBSTR(v_risk_unit_grp_seq_num, 3, 1) = 'A', 'N', SUBSTR(v_risk_unit_grp_seq_num, 3, 1)) AS risk_unit_grp_seq_num_last_pos,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.risk_unit,
	-- *INF*: RTRIM(risk_unit)
	RTRIM(risk_unit) AS risk_unit_out,
	-- *INF*: SUBSTR(risk_unit,1,3)
	-- 
	-- 
	-- ---IIF(source_sys_id = 'PMS',SUBSTR(risk_unit,1,3),
	-- ---IIF(SUBSTR(Policy_key,1,1)='H' and SUBSTR(Policy_key,4,1) = '5',loc_unit_num,SUBSTR(risk_unit,1,3)))
	-- 
	-- ---SUBSTR(risk_unit,1,3)
	SUBSTR(risk_unit, 1, 3) AS risk_unit_First_3pos,
	-- *INF*: IIF(LENGTH(RTRIM(LTRIM(SUBSTR(risk_unit,4,3))))<3,
	-- RPAD(RTRIM(LTRIM(SUBSTR(risk_unit,4,3))),3,'0'), RTRIM(LTRIM(SUBSTR(risk_unit,4,3)))
	-- )
	IFF(
	    LENGTH(RTRIM(LTRIM(SUBSTR(risk_unit, 4, 3)))) < 3,
	    RPAD(RTRIM(LTRIM(SUBSTR(risk_unit, 4, 3))), 3, '0'),
	    RTRIM(LTRIM(SUBSTR(risk_unit, 4, 3)))
	) AS risk_unit_last_3pos,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.risk_unit_seq_num,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.risk_type_ind,
	-- *INF*: IIF(risk_unit_seq_num ='0' and risk_type_ind = 'N/A','00',
	-- IIF(LENGTH(risk_unit_seq_num)=1 and risk_unit_seq_num <> '0' and risk_type_ind = 'N/A', risk_unit_seq_num || '0',risk_unit_seq_num || risk_type_ind ))
	IFF(
	    risk_unit_seq_num = '0' and risk_type_ind = 'N/A', '00',
	    IFF(
	        LENGTH(risk_unit_seq_num) = 1
	    and risk_unit_seq_num <> '0'
	    and risk_type_ind = 'N/A',
	        risk_unit_seq_num || '0',
	        risk_unit_seq_num || risk_type_ind
	    )
	) AS risk_unit_seq_num_out,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.major_peril_code,
	-- *INF*: IIF(major_peril_code='N/A','000',major_peril_code)
	IFF(major_peril_code = 'N/A', '000', major_peril_code) AS major_peril_code_Out,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.major_peril_seq,
	-- *INF*: IIF(major_peril_seq='N/A','00',major_peril_seq)
	IFF(major_peril_seq = 'N/A', '00', major_peril_seq) AS major_peril_seq_out,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.pms_loss_disability,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.reserve_ctgry,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.cause_of_loss,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.pms_type_exposure,
	-- *INF*: IIF(pms_type_exposure = 'N/A','000',pms_type_exposure)
	IFF(pms_type_exposure = 'N/A', '000', pms_type_exposure) AS pms_type_exposure_out,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.pms_type_bureau_code,
	mplt_LM_Policy_n_Claim_Attributes_Reserves.cost_containment_saving_amt,
	EXP_Loss_Master_Offset_Onset_Reserves.sar_id,
	EXP_Loss_Master_Offset_Onset_Reserves.financial_type_code,
	EXP_Loss_Master_Offset_Onset_Reserves.s3p_trans_code,
	EXP_Loss_Master_Offset_Onset_Reserves.pms_trans_code,
	EXP_Loss_Master_Offset_Onset_Reserves.trans_code,
	EXP_Loss_Master_Offset_Onset_Reserves.trans_date,
	EXP_Loss_Master_Offset_Onset_Reserves.pms_acct_entered_date,
	EXP_Loss_Master_Offset_Onset_Reserves.trans_base_type_code,
	EXP_Loss_Master_Offset_Onset_Reserves.trans_ctgry_code,
	EXP_Loss_Master_Offset_Onset_Reserves.Exceed_Trans_Code,
	-- *INF*: TO_CHAR(trans_date,'YYYYMMDD')
	TO_CHAR(trans_date, 'YYYYMMDD') AS V_Trans_Date_char,
	-- *INF*: TO_CHAR(pms_acct_entered_date,'YYYYMMDD')
	-- 
	-- 
	TO_CHAR(pms_acct_entered_date, 'YYYYMMDD') AS V_pms_acct_entered_date_char,
	-- *INF*: SUBSTR(V_Trans_Date_char,1,6)
	-- 
	-- ----IIF(SUBSTR(V_Trans_Date_char,1,6) <= SUBSTR(V_pms_acct_entered_date_char,1,6), 
	-- ----                                SUBSTR(V_pms_acct_entered_date_char,1,6), SUBSTR(V_Trans_Date_char,1,6))
	-- 
	-- 
	-- 
	SUBSTR(V_Trans_Date_char, 1, 6) AS V_Trans_PMS_Account_Date,
	V_Trans_PMS_Account_Date AS Transaction_Account_Date,
	EXP_Loss_Master_Offset_Onset_Reserves.trans_amt,
	EXP_Loss_Master_Offset_Onset_Reserves.trans_hist_amt AS IN_trans_hist_amt,
	-- *INF*: IIF(IN (pms_trans_code,'97','98','99','26','27','81','83','82','84','89','75','76') 
	-- AND source_sys_id <> 'PMS', -1 * IN_trans_hist_amt, IN_trans_hist_amt)
	-- 
	-- --- Added 76 trans_code
	-- 
	IFF(
	    pms_trans_code IN ('97','98','99','26','27','81','83','82','84','89','75','76')
	    and source_sys_id <> 'PMS',
	    - 1 * IN_trans_hist_amt,
	    IN_trans_hist_amt
	) AS v_trans_hist_amt,
	v_trans_hist_amt AS trans_hist_amt,
	EXP_Loss_Master_Offset_Onset_Reserves.source_sys_id,
	-- *INF*: ADD_TO_DATE(SYSDATE,'MM',@{pipeline().parameters.NO_OF_MONTHS})
	DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP) AS V_Last_Month_Date,
	-- *INF*: LAST_DAY(V_Last_Month_Date)
	LAST_DAY(V_Last_Month_Date) AS V_Last_Month_Last_Day_Date,
	-- *INF*: GET_DATE_PART(V_Last_Month_Last_Day_Date,'YYYY')
	DATE_PART(V_Last_Month_Last_Day_Date, 'YYYY') AS V_Account_Date_YYYY,
	-- *INF*: GET_DATE_PART(V_Last_Month_Last_Day_Date,'MM')
	DATE_PART(V_Last_Month_Last_Day_Date, 'MM') AS V_Account_Date_MM,
	-- *INF*: V_Account_Date_YYYY || LPAD(V_Account_Date_MM,2,'0')
	V_Account_Date_YYYY || LPAD(V_Account_Date_MM, 2, '0') AS V_Account_Date,
	-- *INF*: V_Account_Date
	-- ---@{pipeline().parameters.ACCOUNT_DATE}
	-- ---- Date of the previous month, as we are processing previous months data.
	V_Account_Date AS V_Account_Date_To_Check,
	V_Account_Date AS PMS_Account_Date_To_Check,
	-- *INF*: IIF
	-- (
	--     (NOT IN(pms_trans_code,'43','65','66','91') 
	--            AND (V_Trans_PMS_Account_Date = V_Account_Date_To_Check OR SUBSTR(V_pms_acct_entered_date_char,1,6) = V_Account_Date_To_Check )
	--      )
	--    OR 
	--     (IN(pms_trans_code,'90','92','95','97','98','99')  
	--     --AND SUBSTR(source_claim_occurrence_status_code,1,1) = 'O' 
	--     --AND SUBSTR(source_claim_occurrence_status_code,1,3) <> 'OFF'
	--     ),
	-- 'VALID','INVALID'
	-- )
	-- 
	-- ----3/28/2011  OR V_pms_acct_entered_date_char = V_Account_Date_To_Check  : To include the Offset records of exceed claims into calc table.
	-- 
	-- ---- 3/14/2011  Added the condition to check on pms_acct_entered_date for EXCEED Offset_Onset Data
	IFF(
	    (NOT pms_trans_code IN ('43','65','66','91')
	    and (V_Trans_PMS_Account_Date = V_Account_Date_To_Check
	    or SUBSTR(V_pms_acct_entered_date_char, 1, 6) = V_Account_Date_To_Check))
	    or (pms_trans_code IN ('90','92','95','97','98','99')),
	    'VALID',
	    'INVALID'
	) AS V_Valid_Claim_Transaction,
	-- *INF*: IIF(NOT ISNULL(claim_trans_id), 
	--            IIF(V_Valid_Claim_Transaction = 'VALID' AND NOT IN(Exceed_Trans_Code,'E66','E65','B65','B66','R65','R66','S65','S66'),'VALID','INVALID'),
	--              IIF(V_Valid_Claim_Transaction = 'VALID' ,'VALID','INVALID')
	-- )
	-- 
	-- 
	-- ---4/11/2011- Below logic  was changed to the above so that different logic for Claim_Transaction_Records and Claim_Reinsurance_transaction records as there is not s3p_trans_code for Reinsurance_Transactions.
	-- 
	-- ---IIF(V_Valid_Claim_Transaction = 'VALID' AND NOT IN(Exceed_Trans_Code,'E66','E65','B65','B66','R65','R66','S65','S66'),'VALID','INVALID')
	-- 
	-- 
	-- 
	-- -------------------------------------------------
	-- --DECODE(TRUE, 
	-- ---IIF(NOT ISNULL(claim_trans_id),
	-- ---V_Valid_Claim_Transaction = 'VALID' AND NOT IN(Exceed_Trans_Code,'E66','E65','B65','B66','R65','R66','S65','S66'),'VALID',
	-- ---V_Valid_Claim_Transaction = 'VALID','VALID',
	-- --'INVALID')
	-- --)
	-- 
	IFF(
	    claim_trans_id IS NOT NULL,
	    IFF(
	        V_Valid_Claim_Transaction = 'VALID'
	        and NOT Exceed_Trans_Code IN ('E66','E65','B65','B66','R65','R66','S65','S66'),
	        'VALID',
	        'INVALID'
	    ),
	    IFF(
	        V_Valid_Claim_Transaction = 'VALID', 'VALID', 'INVALID'
	    )
	) AS Valid_Claim_Transaction,
	-- *INF*: IIF(IN(pms_trans_code,'90','92','95','97','98','99') AND pms_trans_code <> '95',trans_amt,0.0)
	IFF(
	    pms_trans_code IN ('90','92','95','97','98','99') AND pms_trans_code <> '95', trans_amt, 0.0
	) AS V_LM_Amount_OutStanding,
	-- *INF*: IIF(IN(pms_trans_code,'97','98','99') AND source_sys_id <> 'PMS', -1 * V_LM_Amount_OutStanding,V_LM_Amount_OutStanding)
	-- 
	-- 
	-- --- When Exceed data backfeeds to PMS, B90,B91 become as 99 and sign on the amount field changes to -ve sign aling with value. 
	-- --- Changed the sign of the amount field only for EXCEED EDW Data
	-- ---- 11/12/2010 Added other transaction code 97, 98
	IFF(
	    pms_trans_code IN ('97','98','99') AND source_sys_id <> 'PMS', - 1 * V_LM_Amount_OutStanding,
	    V_LM_Amount_OutStanding
	) AS LM_Amount_OutStanding,
	-- *INF*: IIF(NOT IN(pms_trans_code,'90','92','95','97','98','99') AND NOT IN(pms_trans_code,'71','72','73','74','75','76','77','78',   '79'),trans_amt,0.0)
	IFF(
	    NOT pms_trans_code IN ('90','92','95','97','98','99')
	    and NOT pms_trans_code IN ('71','72','73','74','75','76','77','78','79'),
	    trans_amt,
	    0.0
	) AS V_LM_Amount_Paid_Losses,
	-- *INF*: IIF(IN(pms_trans_code,'26','27','37','81','83','82','84','88','89') AND source_sys_id <>'PMS', 
	-- -1 * V_LM_Amount_Paid_Losses, V_LM_Amount_Paid_Losses)
	-- 
	-- 
	-- 
	-- ---- B22,B23,B24 convert to 82,83,84 into PMS during backfeed and amount field is multiplied by a -ve sign.
	-- ---- 26,27 trans_code in PMS relate to Recovery so the amount field is multiplied by a -ve sign
	IFF(
	    pms_trans_code IN ('26','27','37','81','83','82','84','88','89') AND source_sys_id <> 'PMS',
	    - 1 * V_LM_Amount_Paid_Losses,
	    V_LM_Amount_Paid_Losses
	) AS LM_Amount_Paid_Losses,
	-- *INF*: IIF(NOT IN(pms_trans_code,'90','92','95','97','98','99') AND IN(pms_trans_code,'71','72','73','74','75','76','77','78','79'),trans_amt,0.0)
	IFF(
	    NOT pms_trans_code IN ('90','92','95','97','98','99')
	    and pms_trans_code IN ('71','72','73','74','75','76','77','78','79'),
	    trans_amt,
	    0.0
	) AS V_LM_Amount_Paid_Expenses,
	-- *INF*: IIF(IN(pms_trans_code ,'75','76') AND source_sys_id <>  'PMS',
	-- -1 * V_LM_Amount_Paid_Expenses, V_LM_Amount_Paid_Expenses)
	-- 
	-- ---- R21EX,R22EX,R23EX,R24EX,R29EX converts into 75 when it backfeeds along with amount field being multiplied with -ve sign.
	-- ---12/10/2010  Added 76 to above as R29EX converts to 76 
	IFF(
	    pms_trans_code IN ('75','76') AND source_sys_id <> 'PMS', - 1 * V_LM_Amount_Paid_Expenses,
	    V_LM_Amount_Paid_Expenses
	) AS LM_Amount_Paid_Expenses,
	-- *INF*: IIF(IN(pms_trans_code,'90','92','95','97','98','99') AND pms_trans_code = '95',trans_amt,0.0)
	IFF(
	    pms_trans_code IN ('90','92','95','97','98','99') AND pms_trans_code = '95', trans_amt, 0.0
	) AS V_LM_Unpaid_Loss_Adj_Exp,
	V_LM_Unpaid_Loss_Adj_Exp AS LM_Unpaid_Loss_Adj_Exp,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART( V_Last_Month_Last_Day_Date, 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	-- 
	-- 
	-- --- Changing the date to have 00:00:00 in the timestamp part
	-- 
	DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date)),DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date)),DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date))) AS Loss_Master_Run_Date,
	EXP_Loss_Master_Offset_Onset_Reserves.trans_offset_onset_ind,
	SQL_calculate_reserves.outstandingReserve AS IN_LM_override_amt,
	-- *INF*: IIF(IN(pms_trans_code,'90','92','97','98','99')=1,IN_LM_override_amt ,0.0)
	IFF(pms_trans_code IN ('90','92','97','98','99') = 1, IN_LM_override_amt, 0.0) AS LM_Amount_Outstanding_Override,
	-- *INF*: IIF(IN(pms_trans_code,'90','92','95','97','98','99','71','72','73','74','75','76','77','78', '79')=0,IN_LM_override_amt,0.0)
	IFF(
	    pms_trans_code IN ('90','92','95','97','98','99','71','72','73','74','75','76','77','78','79') = 0,
	    IN_LM_override_amt,
	    0.0
	) AS LM_Amount_Paid_Losses_Override,
	-- *INF*: IIF( IN(pms_trans_code,'71','72','73','74','75','76','77','78','79')=1,IN_LM_override_amt,0.0)
	IFF(
	    pms_trans_code IN ('71','72','73','74','75','76','77','78','79') = 1, IN_LM_override_amt,
	    0.0
	) AS LM_Amount_Paid_Expenses_Override,
	-- *INF*: IIF(pms_trans_code = '95',IN_LM_override_amt,0.0)
	IFF(pms_trans_code = '95', IN_LM_override_amt, 0.0) AS LM_Unpaid_Loss_Adj_Exp_Override
	FROM EXP_Loss_Master_Offset_Onset_Reserves
	 -- Manually join with SQL_calculate_reserves
	 -- Manually join with mplt_LM_Policy_n_Claim_Attributes_Reserves
),
FIL_Claim_Transaction_Rows_Reserves AS (
	SELECT
	EXP_Determine_Loss_Master_Output_Rows_Reserves.claim_trans_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.claim_trans_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.claim_party_occurrence_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.claim_occurrence_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.claimant_cov_det_ak_id AS Claimant_cov_det_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.claim_pay_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.claim_party_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.pol_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.contract_cust_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.agency_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.pol_eff_date, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.pol_exp_date, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.claim_rep_ak_id_H, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.claim_rep_ak_id_E, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.claim_case_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.wc_claimant_det_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.claim_occurrence_key, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.source_claim_occurrence_status_code, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.Policy_key, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.s3p_Claim_Num, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.Claim_loss_date, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.LM_Variation_Code, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.Policy_Symbol, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.Policy_Number, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.Policy_Module, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.loc_unit_num, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.loc_unit_num_out, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.sub_loc_unit_num, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.sub_loc_unit_num_out, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.ins_line, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.ins_line_out, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.risk_unit_grp, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.risk_unit_grp_out, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.risk_unit_grp_seq_num, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.risk_unit_grp_seq_num_First_2pos, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.risk_unit_grp_seq_num_last_pos, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.risk_unit_out AS risk_unit, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.risk_unit_seq_num, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.risk_unit_seq_num_out, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.major_peril_code, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.major_peril_code_Out, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.major_peril_seq, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.major_peril_seq_out, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.pms_loss_disability, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.reserve_ctgry, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.cause_of_loss, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.pms_type_exposure, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.pms_type_exposure_out, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.pms_type_bureau_code, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.sar_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.financial_type_code, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.s3p_trans_code, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.pms_trans_code, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.trans_code, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.trans_date, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.pms_acct_entered_date, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.trans_base_type_code, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.trans_ctgry_code, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.Transaction_Account_Date, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.trans_amt, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.trans_hist_amt, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.source_sys_id, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.PMS_Account_Date_To_Check, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.Valid_Claim_Transaction, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.LM_Amount_Outstanding_Override AS LM_Amount_OutStanding, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.LM_Amount_Paid_Losses_Override AS LM_Amount_Paid_Losses, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.LM_Amount_Paid_Expenses_Override AS LM_Amount_Paid_Expenses, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.LM_Unpaid_Loss_Adj_Exp_Override AS LM_Unpaid_Loss_Adj_Exp, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.Loss_Master_Run_Date, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.cost_containment_saving_amt, 
	EXP_Determine_Loss_Master_Output_Rows_Reserves.trans_offset_onset_ind, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.StatisticalCoverageAKID, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.RatingCoverageAKId AS RatingCoverageAKID, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.PolicySourceID, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.ClassCode, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.SublineCode, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.PolicyCoverageAKID, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.RiskLocationAKID, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.PremiumTransactionAKID, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.BureauStatisticalCodeAKID, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.RiskTerritory, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.StateProvinceCode, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.ZipPostalCode, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.TaxLocation, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.Exposure, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.BureauStatisticalcode1_15, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.BureauSpecialUseCode, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.PMSAnnualStatementLine, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.RatingDateIndicator, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.BureauStatisticalUserLine, 
	mplt_LM_Policy_n_Claim_Attributes_Reserves.AuditReinstatementIndicator
	FROM EXP_Determine_Loss_Master_Output_Rows_Reserves
	 -- Manually join with mplt_LM_Policy_n_Claim_Attributes_Reserves
	WHERE IIF(Valid_Claim_Transaction = 'VALID',TRUE,FALSE)
),
mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves AS (WITH
	INPUT AS (
		
	),
	EXP_Values AS (
		SELECT
		pol_ak_id,
		loss_id,
		ins_line,
		-- *INF*: RTRIM(ins_line)
		RTRIM(ins_line) AS ins_line_out,
		loc_unit_num,
		-- *INF*: RTRIM(loc_unit_num)
		RTRIM(loc_unit_num) AS loc_unit_num1,
		sub_loc_unit_num,
		-- *INF*: RTRIM(sub_loc_unit_num)
		RTRIM(sub_loc_unit_num) AS sub_loc_unit_num1,
		risk_unit_grp,
		-- *INF*: RTRIM(risk_unit_grp)
		RTRIM(risk_unit_grp) AS risk_unit_grp1,
		risk_unit_grp_seq_num_First_2pos,
		-- *INF*: RTRIM(risk_unit_grp_seq_num_First_2pos)
		RTRIM(risk_unit_grp_seq_num_First_2pos) AS risk_unit_grp_seq_num_First_2pos1,
		risk_unit_grp_seq_num_last_pos,
		-- *INF*: RTRIM(risk_unit_grp_seq_num_last_pos)
		RTRIM(risk_unit_grp_seq_num_last_pos) AS risk_unit_grp_seq_num_last_pos1,
		risk_unit_complete,
		-- *INF*: RTRIM(risk_unit_complete)
		RTRIM(risk_unit_complete) AS risk_unit_complete1,
		risk_unit_seq_num,
		-- *INF*: RTRIM(risk_unit_seq_num)
		RTRIM(risk_unit_seq_num) AS risk_unit_seq_num1,
		pms_type_exposure,
		-- *INF*: RTRIM(pms_type_exposure)
		RTRIM(pms_type_exposure) AS pms_type_exposure1,
		major_peril_code,
		-- *INF*: RTRIM(major_peril_code)
		RTRIM(major_peril_code) AS major_peril_code1,
		major_peril_seq,
		-- *INF*: RTRIM(major_peril_seq)
		RTRIM(major_peril_seq) AS major_peril_seq1,
		Claim_loss_date
		FROM INPUT
	),
	LKP_Coverage_Temp_Policy_Transaction AS (
		SELECT
		cov_ak_id,
		temp_pol_trans_ak_id,
		pol_ak_id,
		sar_id,
		ins_line,
		loc_unit_num,
		sub_loc_unit_num,
		risk_unit_grp,
		risk_unit_grp_seq_num_First_2pos,
		risk_unit_grp_seq_num_lastpos,
		risk_unit,
		risk_unit_seq_num,
		major_peril_code,
		major_peril_seq_num,
		pms_type_exposure,
		cov_eff_date,
		type_bureau_code,
		risk_state_prov_code,
		risk_zip_code,
		terr_code,
		tax_loc,
		class_code,
		exposure,
		sub_line_code,
		source_sar_asl,
		source_sar_prdct_line,
		source_sar_sp_use_code,
		source_statistical_code,
		section_code,
		rsn_amended_code,
		part_code,
		rating_date_ind
		FROM (
			SELECT  C.cov_ak_id                     AS cov_ak_id,  
			TPT.temp_pol_trans_ak_id   AS temp_pol_trans_ak_id,  
			CASE  C.pms_type_exposure WHEN 'N/A' THEN '000' ELSE RTRIM(C.pms_type_exposure)   END AS pms_type_exposure,  
			C.type_bureau_code              AS type_bureau_code,  
			TPT.risk_state_prov_code        AS risk_state_prov_code,  
			TPT.risk_zip_code               AS risk_zip_code,  
			TPT.terr_code                   AS terr_code,  
			TPT.tax_loc                     AS tax_loc,  
			TPT.class_code                  AS class_code,  
			TPT.exposure                      AS exposure,  
			TPT.sub_line_code               AS sub_line_code,  
			TPT.source_sar_asl              AS source_sar_asl,  
			TPT.source_sar_prdct_line       AS source_sar_prdct_line,  
			TPT.source_sar_sp_use_code	 AS source_sar_sp_use_code,  
			TPT.source_statistical_code     AS source_statistical_code,  
			TPT.section_code                AS section_code,  
			TPT.rsn_amended_code            AS rsn_amended_code,  
			TPT.part_code                   AS part_code,  
			RTRIM(TPT.rating_date_ind)        AS rating_date_ind,  
			C.pol_ak_id                     AS pol_ak_id,  
			TPT.sar_id                      AS sar_id,  
			CASE C.ins_line WHEN 'N/A' THEN 'NA' ELSE RTRIM(C.ins_line) END AS ins_line,  
			CASE  C.loc_unit_num  WHEN 'N/A' THEN '0000' ELSE RTRIM(C.loc_unit_num)  END AS loc_unit_num,  
			CASE  C.sub_loc_unit_num  WHEN 'N/A' THEN '000' ELSE RTRIM(C.sub_loc_unit_num)   END AS sub_loc_unit_num,  
			CASE  C.risk_unit_grp   WHEN 'N/A' THEN '000' ELSE RTRIM(C.risk_unit_grp)    END AS risk_unit_grp,  
			CASE  SUBSTRING(C.risk_unit_grp_seq_num,1,2)   WHEN 'N/' THEN 'NA' ELSE SUBSTRING(C.risk_unit_grp_seq_num,1,2) END AS risk_unit_grp_seq_num_First_2pos,  
			CASE  SUBSTRING(C.risk_unit_grp_seq_num,3,1)   WHEN 'A' THEN 'N' ELSE SUBSTRING(C.risk_unit_grp_seq_num,3,1) END AS risk_unit_grp_seq_num_lastpos,  
			RTRIM(C.risk_unit)               AS risk_unit,  
			CASE   C.risk_unit_seq_num   WHEN 'N/A' THEN '00' ELSE  RTRIM(C.risk_unit_seq_num)    END AS risk_unit_seq_num,  
			RTRIM(C.major_peril_code)              AS major_peril_code,  
			RTRIM(C.major_peril_seq_num)           AS major_peril_seq_num,  
			C.cov_eff_date                  AS cov_eff_date 
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.coverage C 
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.temp_policy_transaction TPT ON C.cov_ak_id = TPT.cov_ak_id AND C.crrnt_snpsht_flag = 1 AND TPT.crrnt_snpsht_flag = 1
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.Claim_Occurrence CO ON CO.pol_key_ak_id = C.pol_ak_id and CO.crrnt_snpsht_flag = 1
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.Claim_Party_Occurrence CPO ON CPO.Claim_Occurrence_ak_id = CO.Claim_Occurrence_ak_id and CPO.crrnt_snpsht_flag = 1
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.Claimant_Coverage_Detail CCD ON CCD.Claim_Party_Occurrence_ak_id = CPO.Claim_Party_Occurrence_ak_id and CCD.crrnt_snpsht_flag = 1
			AND RTRIM(C.risk_unit) = CCD.Risk_unit AND RTRIM(C.major_peril_code)  = CCD.major_peril_code
			ORDER BY TPT.temp_pol_trans_ak_id   --
			
			--- Order By clause is important in this Lookup Override because how the data is retrieved is important
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,sar_id,ins_line,loc_unit_num,sub_loc_unit_num,risk_unit_grp,risk_unit_grp_seq_num_First_2pos,risk_unit_grp_seq_num_lastpos,risk_unit,risk_unit_seq_num,major_peril_code,major_peril_seq_num,cov_eff_date ORDER BY cov_ak_id DESC) = 1
	),
	EXP_Lkp_Values AS (
		SELECT
		LKP_Coverage_Temp_Policy_Transaction.temp_pol_trans_ak_id,
		LKP_Coverage_Temp_Policy_Transaction.cov_ak_id,
		LKP_Coverage_Temp_Policy_Transaction.pol_ak_id,
		LKP_Coverage_Temp_Policy_Transaction.sar_id,
		LKP_Coverage_Temp_Policy_Transaction.ins_line,
		LKP_Coverage_Temp_Policy_Transaction.loc_unit_num,
		LKP_Coverage_Temp_Policy_Transaction.sub_loc_unit_num,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_First_2pos,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_lastpos,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_seq_num,
		LKP_Coverage_Temp_Policy_Transaction.major_peril_code,
		LKP_Coverage_Temp_Policy_Transaction.major_peril_seq_num,
		LKP_Coverage_Temp_Policy_Transaction.pms_type_exposure,
		LKP_Coverage_Temp_Policy_Transaction.cov_eff_date,
		LKP_Coverage_Temp_Policy_Transaction.type_bureau_code,
		LKP_Coverage_Temp_Policy_Transaction.risk_state_prov_code,
		LKP_Coverage_Temp_Policy_Transaction.risk_zip_code,
		LKP_Coverage_Temp_Policy_Transaction.terr_code,
		LKP_Coverage_Temp_Policy_Transaction.tax_loc,
		LKP_Coverage_Temp_Policy_Transaction.class_code,
		LKP_Coverage_Temp_Policy_Transaction.exposure,
		LKP_Coverage_Temp_Policy_Transaction.sub_line_code,
		LKP_Coverage_Temp_Policy_Transaction.source_sar_asl,
		LKP_Coverage_Temp_Policy_Transaction.source_sar_prdct_line,
		LKP_Coverage_Temp_Policy_Transaction.source_sar_sp_use_code,
		LKP_Coverage_Temp_Policy_Transaction.source_statistical_code,
		LKP_Coverage_Temp_Policy_Transaction.section_code,
		LKP_Coverage_Temp_Policy_Transaction.rsn_amended_code,
		LKP_Coverage_Temp_Policy_Transaction.part_code,
		LKP_Coverage_Temp_Policy_Transaction.rating_date_ind,
		EXP_Values.Claim_loss_date
		FROM EXP_Values
		LEFT JOIN LKP_Coverage_Temp_Policy_Transaction
		ON LKP_Coverage_Temp_Policy_Transaction.pol_ak_id = EXP_Values.pol_ak_id AND LKP_Coverage_Temp_Policy_Transaction.sar_id = EXP_Values.loss_id AND LKP_Coverage_Temp_Policy_Transaction.ins_line = EXP_Values.ins_line_out AND LKP_Coverage_Temp_Policy_Transaction.loc_unit_num = EXP_Values.loc_unit_num1 AND LKP_Coverage_Temp_Policy_Transaction.sub_loc_unit_num = EXP_Values.sub_loc_unit_num1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp = EXP_Values.risk_unit_grp1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_First_2pos = EXP_Values.risk_unit_grp_seq_num_First_2pos1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_lastpos = EXP_Values.risk_unit_grp_seq_num_last_pos1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit = EXP_Values.risk_unit_complete1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_seq_num = EXP_Values.risk_unit_seq_num1 AND LKP_Coverage_Temp_Policy_Transaction.major_peril_code = EXP_Values.major_peril_code1 AND LKP_Coverage_Temp_Policy_Transaction.major_peril_seq_num = EXP_Values.major_peril_seq1 AND LKP_Coverage_Temp_Policy_Transaction.cov_eff_date <= EXP_Values.Claim_loss_date
	),
	OUTPUT AS (
		SELECT
		cov_ak_id AS cov_ak_id_Out, 
		pol_ak_id AS pol_ak_id_Out, 
		temp_pol_trans_ak_id, 
		sar_id AS loss_id_Out, 
		ins_line AS ins_line_Out, 
		loc_unit_num AS loc_unit_num_Out, 
		sub_loc_unit_num AS sub_loc_unit_num_Out, 
		risk_unit_grp AS risk_unit_grp_Out, 
		risk_unit_grp_seq_num_First_2pos AS risk_unit_grp_seq_num_First_2pos_Out, 
		risk_unit_grp_seq_num_lastpos AS risk_unit_grp_seq_num_last_pos_Out, 
		risk_unit AS risk_unit_complete_Out, 
		risk_unit_seq_num AS risk_unit_seq_num_Out, 
		major_peril_code AS major_peril_code_Out, 
		major_peril_seq_num AS major_peril_seq_Out, 
		pms_type_exposure AS pms_type_exposure_Out, 
		cov_eff_date AS cov_eff_date_Out, 
		type_bureau_code AS type_bureau_code_Out, 
		risk_state_prov_code AS risk_state_prov_code_Out, 
		risk_zip_code AS risk_zip_code_Out, 
		terr_code AS terr_code_Out, 
		tax_loc AS tax_loc_Out, 
		class_code AS class_code_Out, 
		exposure AS exposure_Out, 
		sub_line_code AS sub_line_code_Out, 
		source_sar_asl AS source_sar_asl_Out, 
		source_sar_prdct_line AS source_sar_prdct_line_Out, 
		source_sar_sp_use_code, 
		source_statistical_code AS source_statistical_code_Out, 
		source_statistical_line AS source_statistical_line_Out, 
		section_code AS section_code_Out, 
		rsn_amended_code AS rsn_amended_code_Out, 
		part_code AS part_code_Out, 
		rating_date_ind, 
		Claim_loss_date AS Claim_loss_date_Out
		FROM EXP_Lkp_Values
	),
),
EXP_Derive_Values_Reserves AS (
	SELECT
	FIL_Claim_Transaction_Rows_Reserves.claim_party_occurrence_ak_id,
	FIL_Claim_Transaction_Rows_Reserves.claim_occurrence_ak_id,
	FIL_Claim_Transaction_Rows_Reserves.Claimant_cov_det_ak_id AS claimant_cov_det_ak_id,
	FIL_Claim_Transaction_Rows_Reserves.claim_pay_ak_id,
	FIL_Claim_Transaction_Rows_Reserves.claim_trans_id,
	FIL_Claim_Transaction_Rows_Reserves.claim_trans_ak_id,
	FIL_Claim_Transaction_Rows_Reserves.claim_rep_ak_id_H,
	FIL_Claim_Transaction_Rows_Reserves.claim_rep_ak_id_E,
	FIL_Claim_Transaction_Rows_Reserves.claim_case_ak_id,
	-- *INF*: IIF(ISNULL(claim_case_ak_id),-1,claim_case_ak_id)
	IFF(claim_case_ak_id IS NULL, - 1, claim_case_ak_id) AS claim_case_ak_id_out,
	FIL_Claim_Transaction_Rows_Reserves.wc_claimant_det_ak_id,
	-- *INF*: IIF(ISNULL(wc_claimant_det_ak_id),-1,wc_claimant_det_ak_id)
	IFF(wc_claimant_det_ak_id IS NULL, - 1, wc_claimant_det_ak_id) AS wc_claimant_det_ak_id_out,
	FIL_Claim_Transaction_Rows_Reserves.pol_ak_id,
	FIL_Claim_Transaction_Rows_Reserves.agency_ak_id,
	FIL_Claim_Transaction_Rows_Reserves.claim_party_ak_id,
	FIL_Claim_Transaction_Rows_Reserves.contract_cust_ak_id,
	FIL_Claim_Transaction_Rows_Reserves.claim_occurrence_key,
	FIL_Claim_Transaction_Rows_Reserves.Policy_key,
	FIL_Claim_Transaction_Rows_Reserves.Claim_loss_date,
	FIL_Claim_Transaction_Rows_Reserves.Policy_Symbol,
	FIL_Claim_Transaction_Rows_Reserves.Policy_Number,
	FIL_Claim_Transaction_Rows_Reserves.Policy_Module,
	FIL_Claim_Transaction_Rows_Reserves.loc_unit_num,
	FIL_Claim_Transaction_Rows_Reserves.loc_unit_num_out,
	FIL_Claim_Transaction_Rows_Reserves.sub_loc_unit_num,
	FIL_Claim_Transaction_Rows_Reserves.sub_loc_unit_num_out,
	FIL_Claim_Transaction_Rows_Reserves.ins_line,
	FIL_Claim_Transaction_Rows_Reserves.ins_line_out,
	FIL_Claim_Transaction_Rows_Reserves.risk_unit_grp,
	FIL_Claim_Transaction_Rows_Reserves.risk_unit_grp_out,
	FIL_Claim_Transaction_Rows_Reserves.risk_unit_grp_seq_num,
	FIL_Claim_Transaction_Rows_Reserves.risk_unit_grp_seq_num_First_2pos,
	FIL_Claim_Transaction_Rows_Reserves.risk_unit_grp_seq_num_last_pos,
	FIL_Claim_Transaction_Rows_Reserves.risk_unit,
	-- *INF*: SUBSTR(risk_unit,1,3)
	SUBSTR(risk_unit, 1, 3) AS risk_unit_3pos,
	FIL_Claim_Transaction_Rows_Reserves.risk_unit_seq_num,
	FIL_Claim_Transaction_Rows_Reserves.risk_unit_seq_num_out,
	-- *INF*: SUBSTR(risk_unit_seq_num_out,1,1)
	SUBSTR(risk_unit_seq_num_out, 1, 1) AS risk_unit_seq_num_first_pos,
	-- *INF*: SUBSTR(risk_unit_seq_num_out,2,1)
	SUBSTR(risk_unit_seq_num_out, 2, 1) AS risk_unit_seq_num_second_pos,
	FIL_Claim_Transaction_Rows_Reserves.major_peril_code,
	FIL_Claim_Transaction_Rows_Reserves.major_peril_code_Out,
	FIL_Claim_Transaction_Rows_Reserves.major_peril_seq,
	FIL_Claim_Transaction_Rows_Reserves.major_peril_seq_out,
	FIL_Claim_Transaction_Rows_Reserves.pms_loss_disability,
	FIL_Claim_Transaction_Rows_Reserves.reserve_ctgry,
	FIL_Claim_Transaction_Rows_Reserves.cause_of_loss,
	FIL_Claim_Transaction_Rows_Reserves.pms_type_exposure,
	FIL_Claim_Transaction_Rows_Reserves.pms_type_exposure_out,
	FIL_Claim_Transaction_Rows_Reserves.pms_type_bureau_code,
	FIL_Claim_Transaction_Rows_Reserves.sar_id,
	FIL_Claim_Transaction_Rows_Reserves.financial_type_code,
	FIL_Claim_Transaction_Rows_Reserves.pms_trans_code,
	FIL_Claim_Transaction_Rows_Reserves.trans_code,
	FIL_Claim_Transaction_Rows_Reserves.trans_date,
	FIL_Claim_Transaction_Rows_Reserves.pms_acct_entered_date,
	FIL_Claim_Transaction_Rows_Reserves.trans_base_type_code,
	FIL_Claim_Transaction_Rows_Reserves.trans_ctgry_code,
	FIL_Claim_Transaction_Rows_Reserves.Transaction_Account_Date,
	FIL_Claim_Transaction_Rows_Reserves.trans_amt,
	FIL_Claim_Transaction_Rows_Reserves.trans_hist_amt,
	FIL_Claim_Transaction_Rows_Reserves.PMS_Account_Date_To_Check,
	FIL_Claim_Transaction_Rows_Reserves.LM_Variation_Code,
	FIL_Claim_Transaction_Rows_Reserves.LM_Amount_OutStanding,
	FIL_Claim_Transaction_Rows_Reserves.LM_Amount_Paid_Losses,
	FIL_Claim_Transaction_Rows_Reserves.LM_Amount_Paid_Expenses,
	FIL_Claim_Transaction_Rows_Reserves.LM_Unpaid_Loss_Adj_Exp,
	-- *INF*: IIF(NOT ISNULL(temp_pol_trans_ak_id),'VALID','NOTVALID')
	IFF(temp_pol_trans_ak_id IS NOT NULL, 'VALID', 'NOTVALID') AS V_Valid_sar_transaction,
	-- *INF*: IIF(V_Valid_sar_transaction = 'VALID',
	-- IIF(LM_Amount_OutStanding = 0.0 AND LM_Unpaid_Loss_Adj_Exp = 0.0 AND LM_Amount_Paid_Losses=0.0 AND LM_Amount_Paid_Expenses=0.0, 'FILTER','NOFILTER'),'FILTER')
	IFF(
	    V_Valid_sar_transaction = 'VALID',
	    IFF(
	        LM_Amount_OutStanding = 0.0
	        and LM_Unpaid_Loss_Adj_Exp = 0.0
	        and LM_Amount_Paid_Losses = 0.0
	        and LM_Amount_Paid_Expenses = 0.0,
	        'FILTER',
	        'NOFILTER'
	    ),
	    'FILTER'
	) AS V_Transaction_Filter,
	V_Transaction_Filter AS Transaction_Filter,
	FIL_Claim_Transaction_Rows_Reserves.s3p_Claim_Num,
	FIL_Claim_Transaction_Rows_Reserves.source_claim_occurrence_status_code,
	FIL_Claim_Transaction_Rows_Reserves.pol_eff_date,
	FIL_Claim_Transaction_Rows_Reserves.pol_exp_date,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.cov_ak_id_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.temp_pol_trans_ak_id,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.cov_eff_date_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.type_bureau_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.risk_state_prov_code_Out AS risk_state_prov_code,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),StateProvinceCode,risk_state_prov_code)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), StateProvinceCode, risk_state_prov_code) AS risk_state_prov_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.risk_zip_code_Out AS risk_zip_code,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),ZipPostalCode,risk_zip_code)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), ZipPostalCode, risk_zip_code) AS risk_zip_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.terr_code_Out AS terr_code,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),RiskTerritory,terr_code)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), RiskTerritory, terr_code) AS terr_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.tax_loc_Out AS tax_loc,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),TaxLocation,tax_loc)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), TaxLocation, tax_loc) AS tax_loc_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.class_code_Out AS class_code,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),ClassCode,class_code)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), ClassCode, class_code) AS class_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.exposure_Out AS IN_exposure,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),Exposure,IN_exposure)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), Exposure, IN_exposure) AS exposure_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.sub_line_code_Out AS sub_line_code,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),SublineCode,sub_line_code)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), SublineCode, sub_line_code) AS sub_line_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.source_sar_asl_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.source_sar_prdct_line_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.source_sar_sp_use_code,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.source_statistical_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.source_statistical_line_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.section_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.rsn_amended_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.part_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves.rating_date_ind,
	-- *INF*: GET_DATE_PART(trans_date,'MONTH')
	DATE_PART(trans_date, 'MONTH') AS Trans_Date_Month,
	-- *INF*: GET_DATE_PART(trans_date,'DD')
	DATE_PART(trans_date, 'DD') AS Trans_Date_Day,
	-- *INF*: GET_DATE_PART(trans_date,'YYYY')
	DATE_PART(trans_date, 'YYYY') AS Trans_Date_Year,
	-- *INF*: DECODE(TRUE,IN( type_bureau_code_Out,'AL','LP','AI','LI','RL'), '100',
	-- IN( type_bureau_code_Out,'GS','GM','RG'),'400',
	-- IN( type_bureau_code_Out,'WC','WP'),'500',
	-- IN( type_bureau_code_Out,'GL','GI','GN','RQ'),'600',
	-- IN( type_bureau_code_Out,'FF','FM','BF','BP','FT','FP'),'711',
	-- IN( type_bureau_code_Out,'BD'),'722',
	-- IN( type_bureau_code_Out,'BI','BT','RB'),'800')
	DECODE(
	    TRUE,
	    type_bureau_code_Out IN ('AL','LP','AI','LI','RL'), '100',
	    type_bureau_code_Out IN ('GS','GM','RG'), '400',
	    type_bureau_code_Out IN ('WC','WP'), '500',
	    type_bureau_code_Out IN ('GL','GI','GN','RQ'), '600',
	    type_bureau_code_Out IN ('FF','FM','BF','BP','FT','FP'), '711',
	    type_bureau_code_Out IN ('BD'), '722',
	    type_bureau_code_Out IN ('BI','BT','RB'), '800'
	) AS V_Statistical_Line,
	-- *INF*: IIF(ISNULL(V_Statistical_Line),'N/A',V_Statistical_Line)
	IFF(V_Statistical_Line IS NULL, 'N/A', V_Statistical_Line) AS Statistical_Line,
	FIL_Claim_Transaction_Rows_Reserves.cost_containment_saving_amt,
	-- *INF*: IIF(rating_date_ind = 'C', cov_eff_date_Out , pol_eff_date)
	IFF(rating_date_ind = 'C', cov_eff_date_Out, pol_eff_date) AS v_inception_Date,
	v_inception_Date AS inception_Date,
	-- *INF*: DECODE(TRUE, 
	-- IN(risk_state_prov_code,'60','61','62','63','64','65','66','67','68','69','70','71','72','73','74','75','76','77','78','79','80') AND pms_trans_code = '25',1,
	-- IN(pms_trans_code, '22','42'),1,
	-- Transaction_Account_Date = PMS_Account_Date_To_Check AND IN(pms_trans_code, '90','92','23'),1,
	-- Transaction_Account_Date = PMS_Account_Date_To_Check AND pms_trans_code='41',-1,
	-- 0)
	DECODE(
	    TRUE,
	    risk_state_prov_code IN ('60','61','62','63','64','65','66','67','68','69','70','71','72','73','74','75','76','77','78','79','80') AND pms_trans_code = '25', 1,
	    pms_trans_code IN ('22','42'), 1,
	    Transaction_Account_Date = PMS_Account_Date_To_Check AND pms_trans_code IN ('90','92','23'), 1,
	    Transaction_Account_Date = PMS_Account_Date_To_Check AND pms_trans_code = '41', - 1,
	    0
	) AS v_new_claim_count,
	v_new_claim_count AS new_claim_count,
	FIL_Claim_Transaction_Rows_Reserves.Loss_Master_Run_Date,
	FIL_Claim_Transaction_Rows_Reserves.trans_offset_onset_ind,
	FIL_Claim_Transaction_Rows_Reserves.StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	-- *INF*: IIF(ISNULL(i_StatisticalCoverageAKID), -1, i_StatisticalCoverageAKID)
	IFF(i_StatisticalCoverageAKID IS NULL, - 1, i_StatisticalCoverageAKID) AS o_StatisticalCoverageAKID,
	FIL_Claim_Transaction_Rows_Reserves.RatingCoverageAKID AS i_RatingCoverageAKID,
	-- *INF*: IIF(ISNULL(i_RatingCoverageAKID), -1, i_RatingCoverageAKID)
	IFF(i_RatingCoverageAKID IS NULL, - 1, i_RatingCoverageAKID) AS o_RatingCoverageAKID,
	FIL_Claim_Transaction_Rows_Reserves.PolicySourceID,
	FIL_Claim_Transaction_Rows_Reserves.ClassCode,
	FIL_Claim_Transaction_Rows_Reserves.SublineCode,
	FIL_Claim_Transaction_Rows_Reserves.PolicyCoverageAKID,
	FIL_Claim_Transaction_Rows_Reserves.RiskLocationAKID,
	FIL_Claim_Transaction_Rows_Reserves.PremiumTransactionAKID,
	FIL_Claim_Transaction_Rows_Reserves.BureauStatisticalCodeAKID,
	FIL_Claim_Transaction_Rows_Reserves.RiskTerritory,
	FIL_Claim_Transaction_Rows_Reserves.StateProvinceCode,
	FIL_Claim_Transaction_Rows_Reserves.ZipPostalCode,
	FIL_Claim_Transaction_Rows_Reserves.TaxLocation,
	FIL_Claim_Transaction_Rows_Reserves.Exposure,
	FIL_Claim_Transaction_Rows_Reserves.BureauStatisticalcode1_15,
	FIL_Claim_Transaction_Rows_Reserves.BureauSpecialUseCode,
	FIL_Claim_Transaction_Rows_Reserves.PMSAnnualStatementLine,
	FIL_Claim_Transaction_Rows_Reserves.RatingDateIndicator,
	FIL_Claim_Transaction_Rows_Reserves.BureauStatisticalUserLine,
	FIL_Claim_Transaction_Rows_Reserves.AuditReinstatementIndicator
	FROM FIL_Claim_Transaction_Rows_Reserves
	 -- Manually join with mplt_Coverage_Temp_Policy_Transaction_Attributes_Reserves
),
SQ_reinsurance_coverage__Reserves AS (
	SELECT RC.reins_cov_ak_id,
	       RC.pol_ak_id,
	       RC.reins_ins_line,
	       RC.reins_loc_unit_num,
	       RC.reins_sub_loc_unit_num,
	       RC.reins_risk_unit_grp,
	       RC.reins_risk_unit_grp_seq_num,
	       RC.reins_risk_unit,
	       RC.reins_risk_unit_seq_num,
	       RC.reins_section_code,
	       RC.reins_co_num,
	       RC.reins_co_name,
	       RC.reins_eff_date,
	       RC.reins_exp_date,
	       RC.reins_enter_date,
	       RC.reins_type,
	       RC.reins_prcnt_loss_ceded,
	       RC.reins_prcnt_facultative_commssn
	FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage RC
	WHERE  RC.crrnt_snpsht_flag = 1
),
EXP_Reins_Default_Reserves AS (
	SELECT
	reins_cov_ak_id,
	pol_ak_id,
	reins_ins_line,
	reins_loc_unit_num,
	reins_sub_loc_unit_num,
	reins_risk_unit_grp,
	reins_risk_unit_grp_seq_num,
	reins_risk_unit,
	reins_risk_unit_seq_num,
	reins_section_code,
	reins_co_num,
	reins_co_name,
	reins_eff_date,
	reins_exp_date,
	reins_enter_date,
	reins_type,
	reins_prcnt_loss_ceded,
	reins_prcnt_facultative_commssn
	FROM SQ_reinsurance_coverage__Reserves
),
JNR_Reinsurance_Coverage_n_Transaction_Reserves AS (SELECT
	EXP_Reins_Default_Reserves.reins_cov_ak_id, 
	EXP_Reins_Default_Reserves.pol_ak_id, 
	EXP_Reins_Default_Reserves.reins_ins_line, 
	EXP_Reins_Default_Reserves.reins_loc_unit_num, 
	EXP_Reins_Default_Reserves.reins_sub_loc_unit_num, 
	EXP_Reins_Default_Reserves.reins_risk_unit_grp, 
	EXP_Reins_Default_Reserves.reins_risk_unit_grp_seq_num, 
	EXP_Reins_Default_Reserves.reins_risk_unit, 
	EXP_Reins_Default_Reserves.reins_risk_unit_seq_num, 
	EXP_Reins_Default_Reserves.reins_section_code, 
	EXP_Reins_Default_Reserves.reins_co_num, 
	EXP_Reins_Default_Reserves.reins_co_name, 
	EXP_Reins_Default_Reserves.reins_eff_date, 
	EXP_Reins_Default_Reserves.reins_exp_date, 
	EXP_Reins_Default_Reserves.reins_enter_date, 
	EXP_Reins_Default_Reserves.reins_type, 
	EXP_Reins_Default_Reserves.reins_prcnt_loss_ceded, 
	EXP_Reins_Default_Reserves.reins_prcnt_facultative_commssn, 
	EXP_Derive_Values_Reserves.pol_ak_id AS pol_ak_id_out, 
	EXP_Derive_Values_Reserves.section_code_Out, 
	EXP_Derive_Values_Reserves.sar_id, 
	EXP_Derive_Values_Reserves.ins_line_out, 
	EXP_Derive_Values_Reserves.loc_unit_num_out, 
	EXP_Derive_Values_Reserves.sub_loc_unit_num_out, 
	EXP_Derive_Values_Reserves.risk_unit_grp_out, 
	EXP_Derive_Values_Reserves.risk_unit_grp_seq_num, 
	EXP_Derive_Values_Reserves.risk_unit_grp_seq_num_First_2pos, 
	EXP_Derive_Values_Reserves.risk_unit_grp_seq_num_last_pos, 
	EXP_Derive_Values_Reserves.risk_unit, 
	EXP_Derive_Values_Reserves.risk_unit_complete, 
	EXP_Derive_Values_Reserves.risk_unit_seq_num_out, 
	EXP_Derive_Values_Reserves.risk_unit_seq_num_first_pos, 
	EXP_Derive_Values_Reserves.risk_unit_seq_num_second_pos, 
	EXP_Derive_Values_Reserves.major_peril_code_Out, 
	EXP_Derive_Values_Reserves.major_peril_seq_out, 
	EXP_Derive_Values_Reserves.rsn_amended_code_Out, 
	EXP_Derive_Values_Reserves.trans_amt, 
	EXP_Derive_Values_Reserves.trans_hist_amt, 
	EXP_Derive_Values_Reserves.PMS_Account_Date_To_Check, 
	EXP_Derive_Values_Reserves.Claim_loss_date, 
	EXP_Derive_Values_Reserves.pms_trans_code, 
	EXP_Derive_Values_Reserves.risk_unit_3pos, 
	EXP_Derive_Values_Reserves.Policy_Symbol, 
	EXP_Derive_Values_Reserves.Policy_Number, 
	EXP_Derive_Values_Reserves.Policy_Module, 
	EXP_Derive_Values_Reserves.type_bureau_code_Out, 
	EXP_Derive_Values_Reserves.risk_state_prov_code_Out, 
	EXP_Derive_Values_Reserves.risk_zip_code_Out, 
	EXP_Derive_Values_Reserves.terr_code_Out, 
	EXP_Derive_Values_Reserves.tax_loc_Out, 
	EXP_Derive_Values_Reserves.class_code_Out, 
	EXP_Derive_Values_Reserves.exposure_Out, 
	EXP_Derive_Values_Reserves.sub_line_code_Out, 
	EXP_Derive_Values_Reserves.source_sar_asl_Out, 
	EXP_Derive_Values_Reserves.source_sar_prdct_line_Out, 
	EXP_Derive_Values_Reserves.source_sar_sp_use_code, 
	EXP_Derive_Values_Reserves.source_statistical_code_Out, 
	EXP_Derive_Values_Reserves.claim_trans_id, 
	EXP_Derive_Values_Reserves.claim_trans_ak_id, 
	EXP_Derive_Values_Reserves.claimant_cov_det_ak_id, 
	EXP_Derive_Values_Reserves.claim_rep_ak_id_H, 
	EXP_Derive_Values_Reserves.claim_rep_ak_id_E, 
	EXP_Derive_Values_Reserves.cov_ak_id_Out, 
	EXP_Derive_Values_Reserves.pol_ak_id AS pol_ak_id_Out1, 
	EXP_Derive_Values_Reserves.agency_ak_id, 
	EXP_Derive_Values_Reserves.contract_cust_ak_id, 
	EXP_Derive_Values_Reserves.temp_pol_trans_ak_id, 
	EXP_Derive_Values_Reserves.claim_party_occurrence_ak_id, 
	EXP_Derive_Values_Reserves.claim_party_ak_id, 
	EXP_Derive_Values_Reserves.claim_occurrence_ak_id, 
	EXP_Derive_Values_Reserves.claim_pay_ak_id, 
	EXP_Derive_Values_Reserves.claim_case_ak_id_out AS claim_case_ak_id, 
	EXP_Derive_Values_Reserves.wc_claimant_det_ak_id_out AS wc_claimant_det_ak_id, 
	EXP_Derive_Values_Reserves.Statistical_Line, 
	EXP_Derive_Values_Reserves.inception_Date, 
	EXP_Derive_Values_Reserves.new_claim_count, 
	EXP_Derive_Values_Reserves.Loss_Master_Run_Date, 
	EXP_Derive_Values_Reserves.LM_Variation_Code, 
	EXP_Derive_Values_Reserves.LM_Amount_OutStanding, 
	EXP_Derive_Values_Reserves.LM_Amount_Paid_Losses, 
	EXP_Derive_Values_Reserves.LM_Amount_Paid_Expenses, 
	EXP_Derive_Values_Reserves.LM_Unpaid_Loss_Adj_Exp, 
	EXP_Derive_Values_Reserves.trans_date, 
	EXP_Derive_Values_Reserves.pms_acct_entered_date, 
	EXP_Derive_Values_Reserves.trans_offset_onset_ind, 
	EXP_Derive_Values_Reserves.o_StatisticalCoverageAKID AS StatisticalCoverageAKID, 
	EXP_Derive_Values_Reserves.o_RatingCoverageAKID AS RatingCoverageAKID, 
	EXP_Derive_Values_Reserves.financial_type_code, 
	EXP_Derive_Values_Reserves.trans_code, 
	EXP_Derive_Values_Reserves.PolicyCoverageAKID, 
	EXP_Derive_Values_Reserves.RiskLocationAKID, 
	EXP_Derive_Values_Reserves.PremiumTransactionAKID, 
	EXP_Derive_Values_Reserves.BureauStatisticalCodeAKID
	FROM EXP_Derive_Values_Reserves
	INNER JOIN EXP_Reins_Default_Reserves
	ON EXP_Reins_Default_Reserves.pol_ak_id = EXP_Derive_Values_Reserves.pol_ak_id AND EXP_Reins_Default_Reserves.reins_section_code = EXP_Derive_Values_Reserves.section_code_Out
),
EXP_Transform_Statistical_Codes_Reserves AS (
	SELECT
	source_statistical_code_Out AS statistical_code,
	major_peril_code_Out AS major_peril,
	type_bureau_code_Out AS Type_Bureau,
	class_code_Out AS sar_class_code,
	-- *INF*: statistical_code
	-- 
	-- --DECODE(TRUE, Type_Bureau = 'BE', ' '  || statistical_code,
	-- --Type_Bureau = 'BF', ' '  || statistical_code,
	-- --Type_Bureau = 'RP' AND major_peril = '145', ' '  || statistical_code,
	-- --Type_Bureau = 'RL' AND major_peril = '114', '  '  || statistical_code,
	-- --Type_Bureau = 'RL' AND major_peril = '119', '     '  || statistical_code,
	-- --statistical_code)
	-- 
	-- ---- Had to introduce space at the begining of the field because of LTRIM(RTRIM)) to statistical codes in Temp_Policy_transaction Table.
	statistical_code AS v_statistical_code,
	'D' AS v_stat_plan_id,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,1,1))=0,' ',SUBSTR(v_statistical_code,1,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 1, 1)) = 0, ' ', SUBSTR(v_statistical_code, 1, 1)) AS v_pos_1,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,2,1))=0,' ',SUBSTR(v_statistical_code,2,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 2, 1)) = 0, ' ', SUBSTR(v_statistical_code, 2, 1)) AS v_pos_2,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,3,1))=0,' ',SUBSTR(v_statistical_code,3,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 3, 1)) = 0, ' ', SUBSTR(v_statistical_code, 3, 1)) AS v_pos_3,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,4,1))=0,' ',SUBSTR(v_statistical_code,4,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 4, 1)) = 0, ' ', SUBSTR(v_statistical_code, 4, 1)) AS v_pos_4,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,5,1))=0,' ',SUBSTR(v_statistical_code,5,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 5, 1)) = 0, ' ', SUBSTR(v_statistical_code, 5, 1)) AS v_pos_5,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,6,1))=0,' ',SUBSTR(v_statistical_code,6,1))
	-- 
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 6, 1)) = 0, ' ', SUBSTR(v_statistical_code, 6, 1)) AS v_pos_6,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,7,1))=0,' ',SUBSTR(v_statistical_code,7,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 7, 1)) = 0, ' ', SUBSTR(v_statistical_code, 7, 1)) AS v_pos_7,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,8,1))=0,' ',SUBSTR(v_statistical_code,8,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 8, 1)) = 0, ' ', SUBSTR(v_statistical_code, 8, 1)) AS v_pos_8,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,9,1))=0,' ',SUBSTR(v_statistical_code,9,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 9, 1)) = 0, ' ', SUBSTR(v_statistical_code, 9, 1)) AS v_pos_9,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,10,1))=0,' ',SUBSTR(v_statistical_code,10,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 10, 1)) = 0, ' ', SUBSTR(v_statistical_code, 10, 1)) AS v_pos_10,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,11,1))=0,' ',SUBSTR(v_statistical_code,11,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 11, 1)) = 0, ' ', SUBSTR(v_statistical_code, 11, 1)) AS v_pos_11,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,12,1))=0,' ',SUBSTR(v_statistical_code,12,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 12, 1)) = 0, ' ', SUBSTR(v_statistical_code, 12, 1)) AS v_pos_12,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,13,1))=0,' ',SUBSTR(v_statistical_code,13,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 13, 1)) = 0, ' ', SUBSTR(v_statistical_code, 13, 1)) AS v_pos_13,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,14,1))=0,' ',SUBSTR(v_statistical_code,14,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 14, 1)) = 0, ' ', SUBSTR(v_statistical_code, 14, 1)) AS v_pos_14,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,15,1))=0,' ',SUBSTR(v_statistical_code,15,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 15, 1)) = 0, ' ', SUBSTR(v_statistical_code, 15, 1)) AS v_pos_15,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,16,1))=0,' ',SUBSTR(v_statistical_code,16,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 16, 1)) = 0, ' ', SUBSTR(v_statistical_code, 16, 1)) AS v_pos_16,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,17,1))=0,' ',SUBSTR(v_statistical_code,17,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 17, 1)) = 0, ' ', SUBSTR(v_statistical_code, 17, 1)) AS v_pos_17,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,18,1))=0,' ',SUBSTR(v_statistical_code,18,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 18, 1)) = 0, ' ', SUBSTR(v_statistical_code, 18, 1)) AS v_pos_18,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,19,1))=0,' ',SUBSTR(v_statistical_code,19,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 19, 1)) = 0, ' ', SUBSTR(v_statistical_code, 19, 1)) AS v_pos_19,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,20,1))=0,' ',SUBSTR(v_statistical_code,20,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 20, 1)) = 0, ' ', SUBSTR(v_statistical_code, 20, 1)) AS v_pos_20,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,21,1))=0,' ',SUBSTR(v_statistical_code,21,1))
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,21,1))=0,' ',SUBSTR(v_statistical_code,21,1))
	-- 
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(
	    TRUE,
	    Type_Bureau = 'RP', '0',
	    LENGTH(SUBSTR(v_statistical_code, 21, 1)) = 0, ' ',
	    SUBSTR(v_statistical_code, 21, 1)
	) AS v_pos_21,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,22,1))=0,' ',SUBSTR(v_statistical_code,22,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,22,1))=0,' ',SUBSTR(v_statistical_code,22,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(
	    TRUE,
	    Type_Bureau = 'RP', '0',
	    LENGTH(SUBSTR(v_statistical_code, 22, 1)) = 0, ' ',
	    SUBSTR(v_statistical_code, 22, 1)
	) AS v_pos_22,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,23,1))=0,' ',SUBSTR(v_statistical_code,23,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,23,1))=0,' ',SUBSTR(v_statistical_code,23,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(
	    TRUE,
	    Type_Bureau = 'RP', '0',
	    LENGTH(SUBSTR(v_statistical_code, 23, 1)) = 0, ' ',
	    SUBSTR(v_statistical_code, 23, 1)
	) AS v_pos_23,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,24,1))=0,' ',SUBSTR(v_statistical_code,24,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,24,1))=0,' ',SUBSTR(v_statistical_code,24,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(
	    TRUE,
	    Type_Bureau = 'RP', '0',
	    LENGTH(SUBSTR(v_statistical_code, 24, 1)) = 0, ' ',
	    SUBSTR(v_statistical_code, 24, 1)
	) AS v_pos_24,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','{',
	-- LENGTH(SUBSTR(v_statistical_code,25,1))=0,' ',SUBSTR(v_statistical_code,25,1))
	-- 
	-- --- IN COBOL "{" represents a  +ve sign and "}" is -ve sign, since this is base rate for Type_Bureau RP is a sign field so COBOL creates "{". Replicating the COBOL logic.
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '{'
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,25,1))=0,' ',SUBSTR(v_statistical_code,25,1))
	DECODE(
	    TRUE,
	    Type_Bureau = 'RP', '{',
	    LENGTH(SUBSTR(v_statistical_code, 25, 1)) = 0, ' ',
	    SUBSTR(v_statistical_code, 25, 1)
	) AS v_pos_25,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,26,1))=0,' ',SUBSTR(v_statistical_code,26,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 26, 1)) = 0, ' ', SUBSTR(v_statistical_code, 26, 1)) AS v_pos_26,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,27,1))=0,' ',SUBSTR(v_statistical_code,27,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 27, 1)) = 0, ' ', SUBSTR(v_statistical_code, 27, 1)) AS v_pos_27,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,28,1))=0,' ',SUBSTR(v_statistical_code,28,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 28, 1)) = 0, ' ', SUBSTR(v_statistical_code, 28, 1)) AS v_pos_28,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,29,1))=0,' ',SUBSTR(v_statistical_code,29,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 29, 1)) = 0, ' ', SUBSTR(v_statistical_code, 29, 1)) AS v_pos_29,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,30,1))=0,' ',SUBSTR(v_statistical_code,30,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 30, 1)) = 0, ' ', SUBSTR(v_statistical_code, 30, 1)) AS v_pos_30,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,31,1))=0,' ',SUBSTR(v_statistical_code,31,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 31, 1)) = 0, ' ', SUBSTR(v_statistical_code, 31, 1)) AS v_pos_31,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,32,1))=0,' ',SUBSTR(v_statistical_code,32,1))
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 32, 1)) = 0, ' ', SUBSTR(v_statistical_code, 32, 1)) AS v_pos_32,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,33,1))=0,' ',SUBSTR(v_statistical_code,33,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 33, 1)) = 0, ' ', SUBSTR(v_statistical_code, 33, 1)) AS v_pos_33,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,34,1))=0,' ',SUBSTR(v_statistical_code,34,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 34, 1)) = 0, ' ', SUBSTR(v_statistical_code, 34, 1)) AS v_pos_34,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,35,1))=0,' ',SUBSTR(v_statistical_code,35,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 35, 1)) = 0, ' ', SUBSTR(v_statistical_code, 35, 1)) AS v_pos_35,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,36,1))=0,' ',SUBSTR(v_statistical_code,36,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 36, 1)) = 0, ' ', SUBSTR(v_statistical_code, 36, 1)) AS v_pos_36,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,37,1))=0,' ',SUBSTR(v_statistical_code,37,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 37, 1)) = 0, ' ', SUBSTR(v_statistical_code, 37, 1)) AS v_pos_37,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,38,1))=0,' ',SUBSTR(v_statistical_code,38,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 38, 1)) = 0, ' ', SUBSTR(v_statistical_code, 38, 1)) AS v_pos_38,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS Generic,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Code_AC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_AI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_26 || '       ' || v_pos_25 || v_pos_23  || v_pos_24  || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22)
	-- 
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_26 || '       ' || v_pos_25 || v_pos_23 || v_pos_24 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22) AS v_Stat_Codes_AL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10  || v_pos_11|| v_pos_20 || v_pos_21  || 
	-- '             ' ||  v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19  )
	-- 
	--  -----It has a Filler of 13 spaces
	-- --- I have checked this code this is fine
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_20 || v_pos_21 || '             ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19) AS v_Stat_Codes_AN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 ||
	-- '      ' || v_pos_14 || v_pos_23  || v_pos_24  || '  '  ||  v_pos_26  || v_pos_27  || v_pos_28  || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || '      ' || v_pos_14 || v_pos_23 || v_pos_24 || '  ' || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22) AS v_Stat_Codes_AP,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_11 || v_pos_10 || 
	--   v_pos_12 || v_pos_13 )
	-- 
	-- --- Verified the logic
	-- 
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_11 || v_pos_10 || v_pos_12 || v_pos_13) AS v_Stat_Codes_A2,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_11 || v_pos_12 )
	-- 
	-- --- Verified logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_11 || v_pos_12) AS v_Stat_Codes_A3,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 ||
	-- '           '  ||  v_pos_22 || v_pos_29 || '  ' || v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28)
	-- 
	-- --- Verified logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || '           ' || v_pos_22 || v_pos_29 || '  ' || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28) AS v_Stat_Codes_BB,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17  || v_pos_20  || v_pos_27  || v_pos_28  || v_pos_29 || '    ' ||v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26 )
	-- 
	-- 
	-- -- Verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_20 || v_pos_27 || v_pos_28 || v_pos_29 || '    ' || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26) AS v_Stat_Codes_BC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- --- Verified logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_BD,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 ||  v_pos_6 || v_pos_7 || '                    ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13)
	-- 
	-- 
	--  ---  Verified Logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || '                    ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13) AS v_Stat_Codes_BE,
	-- *INF*: ('  '  || v_pos_4  || v_pos_5 || ' ' || v_pos_14 || '  ' || v_pos_15 || v_pos_16 || '   ' ||  v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || '     ' || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22 || '   ')
	-- 
	-- 
	-- --8/22/2011 - Added 2 spaces in the beginning. In COBOL, statitistical code field is initialised to spaces at the start of reformatting. If there is no code to move certain fields then the spaces stay as it is except other fileds are layed out over spaces.
	-- --- Verified the logic
	-- 
	('  ' || v_pos_4 || v_pos_5 || ' ' || v_pos_14 || '  ' || v_pos_15 || v_pos_16 || '   ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || '     ' || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || '   ') AS v_Stat_Codes_BF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_4  || v_pos_5)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_4 || v_pos_5) AS v_Stat_Codes_BP,
	-- *INF*: (v_pos_1 || v_pos_2 )
	-- 
	-- --- Verified the logic
	(v_pos_1 || v_pos_2) AS v_Stat_Codes_BI,
	-- *INF*: v_pos_1
	-- 
	-- -- verified the logic
	v_pos_1 AS v_Stat_Codes_BL,
	-- *INF*: (SUBSTR(sar_class_code,1,3) || '  ' || v_pos_18  ||  v_pos_19 || v_pos_1 ||  ' ' ||  v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 
	-- || '    ' ||  v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34 || v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28 || '   ' )
	-- 
	-- --- Verfied the logic
	(SUBSTR(sar_class_code, 1, 3) || '  ' || v_pos_18 || v_pos_19 || v_pos_1 || ' ' || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || '    ' || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || '   ') AS v_Stat_Codes_BM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || '      '  ||  v_pos_8 || v_pos_9 || '           ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19)
	-- 
	--  ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '      ' || v_pos_8 || v_pos_9 || '           ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19) AS v_Stat_Codes_BT,
	-- *INF*: (v_pos_1 || v_pos_2 || '      '  || v_pos_9 || v_pos_10 || v_pos_17 || v_pos_18 ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || '      ' || v_pos_9 || v_pos_10 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31) AS v_Stat_Codes_B2,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17)
	-- 
	-- ----- verified the logic
	-- 
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17) AS v_Stat_Codes_CC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || 
	--  v_pos_17 || v_pos_18  || ' ' ||  v_pos_20 || '              ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_17 || v_pos_18 || ' ' || v_pos_20 || '              ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_CF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- ---- Generic 
	-- -- No Change from Input copybook to Output
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_CR,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_6 || v_pos_7 || ' '  || v_pos_9 || '  ' || v_pos_12 || ' ' || v_pos_14 || v_pos_15 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_6 || v_pos_7 || ' ' || v_pos_9 || '  ' || v_pos_12 || ' ' || v_pos_14 || v_pos_15) AS v_Stat_Codes_CI,
	-- *INF*: (v_pos_1 || v_pos_4  || v_pos_6 || v_pos_7 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_4 || v_pos_6 || v_pos_7) AS v_Stat_Codes_CL,
	-- *INF*: ('  ' || v_pos_1 || v_pos_2 || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- ---- verified the logic
	('  ' || v_pos_1 || v_pos_2 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_CP,
	-- *INF*: (v_pos_3 || v_pos_4  || v_pos_5 )
	-- 
	-- ---- verified the logic
	(v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_CN,
	-- *INF*: v_pos_1
	-- 
	-- -----
	v_pos_1 AS v_Stat_Codes_EI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || '                   ' ||v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16)
	-- 
	-- ---- verified the logic
	-- --- 19 spaces
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_EQ,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4) AS v_Stat_Codes_FC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 
	-- || '                  ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	-- ---- 18 Spaces
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '                  ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_FF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_FM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16)
	-- 
	-- ---- verified the logic
	-- --- 19 spaces
	-- 
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_FO,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3) AS v_Stat_Codes_FP,
	-- *INF*: (v_pos_1 || v_pos_2 || ' ' || v_pos_3 || '  ' || v_pos_6 || v_pos_7 || '   ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 ||
	-- '       ' || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22 || '   ')
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || ' ' || v_pos_3 || '  ' || v_pos_6 || v_pos_7 || '   ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || '       ' || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || '   ') AS v_Stat_Codes_FT,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_12 || v_pos_13 || '                '  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9)
	-- 
	-- ---- verified the logic
	-- -- 17 Spaces
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_12 || v_pos_13 || '                ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9) AS v_Stat_Codes_GI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_4  || v_pos_5  || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || '      ' || v_pos_13 || v_pos_29  || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19 ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_4 || v_pos_5 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || '      ' || v_pos_13 || v_pos_29 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28) AS v_Stat_Codes_GL,
	-- *INF*: (v_pos_1 || '           '  ||   v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- ---- verified the logic
	-- 
	(v_pos_1 || '           ' || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_GP,
	-- *INF*: (v_pos_1 || '                       ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_11 || v_pos_12 || v_pos_13)
	-- 
	-- ---- verified the logic
	-- --- 23 spaces
	-- 
	-- 
	-- 
	(v_pos_1 || '                       ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_11 || v_pos_12 || v_pos_13) AS v_Stat_Codes_GS,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_18  ||  v_pos_19  
	-- || ' ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || '                ')
	-- 
	-- 
	-- ---- verified the logic
	-- --- 16 Spaces at the end
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_18 || v_pos_19 || ' ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || '                ') AS v_Stat_Codes_HO,
	-- *INF*: ('        ' || v_pos_11 || v_pos_12 || '               '  || v_pos_4  || v_pos_5  || v_pos_6  || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_14 || v_pos_15 || v_pos_17)
	-- 
	-- ---- verified the logic
	('        ' || v_pos_11 || v_pos_12 || '               ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_14 || v_pos_15 || v_pos_17) AS v_Stat_Codes_IM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  || v_pos_24  || v_pos_25  || v_pos_26 || v_pos_28  || v_pos_29  || v_pos_30 || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35) AS v_Stat_Codes_JR,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_ME,
	-- *INF*: (v_pos_1 || ' '  || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' ||  v_pos_11 || v_pos_12 || v_pos_13 || '  ' || v_pos_18  || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || '                ' ) 
	-- 
	-- --- need logic for stat-plan -id
	-- ---- 16 Spaces at the end
	(v_pos_1 || ' ' || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || '  ' || v_pos_18 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || '                ') AS v_Stat_Codes_MH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || '                  '  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	--  --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || '                  ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_MI,
	-- *INF*: (v_pos_6 || v_pos_7 || v_pos_3 || v_pos_4  || v_pos_2 || '      ' || v_pos_1 || '        ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || ' ' ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24 )
	-- 
	--  --- verified the logic
	(v_pos_6 || v_pos_7 || v_pos_3 || v_pos_4 || v_pos_2 || '      ' || v_pos_1 || '        ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || ' ' || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24) AS v_Stat_Codes_ML,
	-- *INF*: -- No Stats code in the Output Copybook just the policy_type logic
	'' AS v_Stat_Codes_MP,
	-- *INF*: (SUBSTR(sar_class_code,1,3) || v_pos_17 || v_pos_18 ||  v_pos_19  || v_pos_1 || v_pos_2 || v_pos_3 || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || '   ' || '       ' || '      ' )
	-- 
	-- --- Need to look at complete logic
	-- 
	(SUBSTR(sar_class_code, 1, 3) || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_1 || v_pos_2 || v_pos_3 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '   ' || '       ' || '      ') AS v_Stat_Codes_M2,
	-- *INF*: ( '                 ' || v_stat_plan_id)
	-- 
	-- ----verified the logic
	('                 ' || v_stat_plan_id) AS v_Stat_Codes_NE,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  ||  v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_19)
	-- 
	-- --- Verified the Logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_19) AS v_Stat_Codes_PC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || v_pos_19  || v_pos_20  ||  v_pos_21)
	-- 
	-- --- verified the logic
	--  
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || v_pos_19 || v_pos_20 || v_pos_21) AS v_Stat_Codes_PH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_PF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_PI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_PL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 ||  v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18) AS v_Stat_Codes_PM,
	-- *INF*: (v_pos_1 || v_pos_2)
	-- 
	-- --- verified the logic
	-- 
	(v_pos_1 || v_pos_2) AS v_Stat_Codes_RB,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3) AS v_Stat_Codes_RG,
	-- *INF*: (v_pos_1 || v_pos_2)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2) AS v_Stat_Codes_RI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24) AS v_Stat_Codes_RL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10) AS v_Stat_Codes_RM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || ' ' || 
	-- v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21 || v_pos_22 ||  v_pos_23  || v_pos_24)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24) AS v_Stat_Codes_RN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29 || v_pos_30 || v_pos_31|| v_pos_33 || v_pos_34  ||  v_pos_35  || v_pos_32)
	-- 
	-- ----
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_32) AS v_Stat_Codes_RP,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5 )
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_RQ,
	-- *INF*: (v_pos_1 || ' ' || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 )
	-- 
	-- --- verified the logic
	(v_pos_1 || ' ' || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8) AS v_Stat_Codes_SM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_8 || v_pos_11 || v_pos_9)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_8 || v_pos_11 || v_pos_9) AS v_Stat_Codes_TH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 
	-- || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19
	-- ||  v_pos_22  ||  v_pos_23  || v_pos_24 || '       ' || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_22 || v_pos_23 || v_pos_24 || '       ' || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36) AS v_Stat_Codes_VL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19 
	--  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30 || ' ' || v_pos_32  ||  v_pos_33
	-- || v_pos_34  ||  v_pos_35  || v_pos_36 )
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || ' ' || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36) AS v_Stat_Codes_VP,
	-- *INF*: ('   ' || v_pos_4  || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12  || ' ' || v_pos_14 || v_pos_15 || '              ' 
	-- || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34 || v_pos_35)
	-- 
	-- --- verified the logic
	('   ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || ' ' || v_pos_14 || v_pos_15 || '              ' || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35) AS v_Stat_Codes_VN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  
	-- || ' ' || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || '    ' || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || ' ' || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || '    ' || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Codes_VC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31)
	-- 
	--  --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31) AS v_Stat_Codes_WC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_WP,
	-- *INF*: ('   ' || v_pos_1 || v_pos_2 || '            ' || v_stat_plan_id)
	-- 
	-- --8/19/2011 Added v_stat_plan_id
	-- --- need to bring stat plan_id
	--  --- verified the logic but need stat plan id
	-- 
	('   ' || v_pos_1 || v_pos_2 || '            ' || v_stat_plan_id) AS v_Stat_Codes_WL,
	-- *INF*: DECODE(Type_Bureau, 'AC', v_Stat_Code_AC, 'AI', v_Stat_Codes_AI, 'AL', v_Stat_Codes_AL, 'AN', v_Stat_Codes_AN, 'AP', v_Stat_Codes_AP, 'A2', v_Stat_Codes_A2, 'A3', v_Stat_Codes_A3, 'BB', v_Stat_Codes_BB, 'BC', v_Stat_Codes_BC, 'BD', v_Stat_Codes_BD, 'BE', v_Stat_Codes_BE, 'BF', v_Stat_Codes_BF, 'BP', v_Stat_Codes_BP, 'BI', v_Stat_Codes_BI, 'BL', v_Stat_Codes_BL, 'BM', v_Stat_Codes_BM, 'BT', v_Stat_Codes_BT, 'B2', v_Stat_Codes_B2, 'CC', v_Stat_Codes_CC, 'CF', v_Stat_Codes_CF, 'CI', v_Stat_Codes_CI, 'CL', v_Stat_Codes_CL, 'CN', v_Stat_Codes_CN, 'CP', v_Stat_Codes_CP, 'EI', v_Stat_Codes_EI, 'EQ', v_Stat_Codes_EQ, 'FC', v_Stat_Codes_FC, 'FF', v_Stat_Codes_FF, 'FM', v_Stat_Codes_FM, 'FO', v_Stat_Codes_FO, 'FP', v_Stat_Codes_FP, 'FT', v_Stat_Codes_FT, 'GI', v_Stat_Codes_GI, 'GL', v_Stat_Codes_GL, 'GP', v_Stat_Codes_GP, 'GS', v_Stat_Codes_GS, 'HO', v_Stat_Codes_HO, 'IM', v_Stat_Codes_IM, 'JR', v_Stat_Codes_JR, 'ME', v_Stat_Codes_ME, 'MH', v_Stat_Codes_MH, 'MI', v_Stat_Codes_MI, 'ML',
	-- v_Stat_Codes_ML, 'MP', v_Stat_Codes_MP, 'M2', v_Stat_Codes_M2, 'NE', v_Stat_Codes_NE, 'PC', v_Stat_Codes_PC, 'PH', v_Stat_Codes_PH, 'PM', v_Stat_Codes_PM, 'RB', v_Stat_Codes_RB, 'RG', v_Stat_Codes_RG, 'RI', v_Stat_Codes_RI, 'RL', v_Stat_Codes_RL, 'RM', v_Stat_Codes_RM, 'RN', v_Stat_Codes_RN, 'RP', v_Stat_Codes_RP, 'RQ', v_Stat_Codes_RQ, 'SM', v_Stat_Codes_SM, 'TH', v_Stat_Codes_TH, 'VL', v_Stat_Codes_VL, 'VP', v_Stat_Codes_VP, 'VN', v_Stat_Codes_VN, 'VC', v_Stat_Codes_VC, 'WC', v_Stat_Codes_WC, 'WL', v_Stat_Codes_WL,
	-- 'CR', v_Stat_Code_CR, 'PF', v_Stat_Code_PF,'PI', v_Stat_Code_PI, 'PL', v_Stat_Code_PL,
	-- 'WP', v_Stat_Code_WP,v_statistical_code) 
	DECODE(
	    Type_Bureau,
	    'AC', v_Stat_Code_AC,
	    'AI', v_Stat_Codes_AI,
	    'AL', v_Stat_Codes_AL,
	    'AN', v_Stat_Codes_AN,
	    'AP', v_Stat_Codes_AP,
	    'A2', v_Stat_Codes_A2,
	    'A3', v_Stat_Codes_A3,
	    'BB', v_Stat_Codes_BB,
	    'BC', v_Stat_Codes_BC,
	    'BD', v_Stat_Codes_BD,
	    'BE', v_Stat_Codes_BE,
	    'BF', v_Stat_Codes_BF,
	    'BP', v_Stat_Codes_BP,
	    'BI', v_Stat_Codes_BI,
	    'BL', v_Stat_Codes_BL,
	    'BM', v_Stat_Codes_BM,
	    'BT', v_Stat_Codes_BT,
	    'B2', v_Stat_Codes_B2,
	    'CC', v_Stat_Codes_CC,
	    'CF', v_Stat_Codes_CF,
	    'CI', v_Stat_Codes_CI,
	    'CL', v_Stat_Codes_CL,
	    'CN', v_Stat_Codes_CN,
	    'CP', v_Stat_Codes_CP,
	    'EI', v_Stat_Codes_EI,
	    'EQ', v_Stat_Codes_EQ,
	    'FC', v_Stat_Codes_FC,
	    'FF', v_Stat_Codes_FF,
	    'FM', v_Stat_Codes_FM,
	    'FO', v_Stat_Codes_FO,
	    'FP', v_Stat_Codes_FP,
	    'FT', v_Stat_Codes_FT,
	    'GI', v_Stat_Codes_GI,
	    'GL', v_Stat_Codes_GL,
	    'GP', v_Stat_Codes_GP,
	    'GS', v_Stat_Codes_GS,
	    'HO', v_Stat_Codes_HO,
	    'IM', v_Stat_Codes_IM,
	    'JR', v_Stat_Codes_JR,
	    'ME', v_Stat_Codes_ME,
	    'MH', v_Stat_Codes_MH,
	    'MI', v_Stat_Codes_MI,
	    'ML', v_Stat_Codes_ML,
	    'MP', v_Stat_Codes_MP,
	    'M2', v_Stat_Codes_M2,
	    'NE', v_Stat_Codes_NE,
	    'PC', v_Stat_Codes_PC,
	    'PH', v_Stat_Codes_PH,
	    'PM', v_Stat_Codes_PM,
	    'RB', v_Stat_Codes_RB,
	    'RG', v_Stat_Codes_RG,
	    'RI', v_Stat_Codes_RI,
	    'RL', v_Stat_Codes_RL,
	    'RM', v_Stat_Codes_RM,
	    'RN', v_Stat_Codes_RN,
	    'RP', v_Stat_Codes_RP,
	    'RQ', v_Stat_Codes_RQ,
	    'SM', v_Stat_Codes_SM,
	    'TH', v_Stat_Codes_TH,
	    'VL', v_Stat_Codes_VL,
	    'VP', v_Stat_Codes_VP,
	    'VN', v_Stat_Codes_VN,
	    'VC', v_Stat_Codes_VC,
	    'WC', v_Stat_Codes_WC,
	    'WL', v_Stat_Codes_WL,
	    'CR', v_Stat_Code_CR,
	    'PF', v_Stat_Code_PF,
	    'PI', v_Stat_Code_PI,
	    'PL', v_Stat_Code_PL,
	    'WP', v_Stat_Code_WP,
	    v_statistical_code
	) AS V_Formatted_Stat_Codes,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,1,25)
	SUBSTR(V_Formatted_Stat_Codes, 1, 25) AS Formatted_Stat_Codes,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,26,9)
	SUBSTR(V_Formatted_Stat_Codes, 26, 9) AS Formatted_Stat_Codes_26_34,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,35,4)
	SUBSTR(V_Formatted_Stat_Codes, 35, 4) AS Formatted_Stat_Codes_34_38,
	-- *INF*: DECODE(Type_Bureau,'AI', (v_pos_11 || v_pos_12),
	-- 'AL', (v_pos_15  ||  v_pos_16),
	-- 'AN',(v_pos_12 || v_pos_13),
	-- 'AP',(v_pos_12 || v_pos_13),
	-- 'A2',(v_pos_8 || v_pos_9),
	-- 'A3',(v_pos_8 || v_pos_9),
	-- 'BB',(v_pos_20 || v_pos_21),
	-- 'BC',(v_pos_18 || v_pos_19),
	-- 'BE', ( v_pos_4  || v_pos_5),
	-- 'BF', (v_pos_1  ||  v_pos_2),
	-- 'BP', (' '  ||  v_pos_2),
	-- 'BI', (v_pos_3 ||  v_pos_4),
	-- 'BL', (v_pos_3  ||  v_pos_4),
	-- 'BM',(v_pos_20 || v_pos_21),
	-- 'BT', (v_pos_11  ||  v_pos_12),
	-- 'B2',(v_pos_14  ||  v_pos_15),
	-- 'CF', (v_pos_8  || v_pos_9),
	-- 'CI',(v_pos_3  ||  v_pos_4),
	-- 'CN', (v_pos_1  ||  v_pos_2),
	-- 'CP', (v_pos_3  ||  v_pos_4),
	-- 'EI', (v_pos_2  ||  v_pos_3),
	-- 'EQ', (v_pos_8  || v_pos_9),
	-- 'FF', (v_pos_8  || v_pos_9),
	-- 'FI', (v_pos_1  ||  v_pos_2),
	-- 'FM', (v_pos_6  ||  v_pos_7),
	-- 'FO', (v_pos_8  || v_pos_9),
	-- 'FP', (v_pos_2  ||  v_pos_3),
	-- 'FT', (v_pos_4  ||  v_pos_5),
	-- 'GI', (v_pos_10  ||  v_pos_11),
	-- 'GL',(v_pos_20 || v_pos_21),
	-- 'GM', (v_pos_1  ||  v_pos_2),
	-- 'GP', (v_pos_8  || v_pos_9),
	-- 'GS',(v_pos_3  ||  v_pos_4),
	-- 'II', (v_pos_1  ||  v_pos_2),
	-- 'IM', (v_pos_1  ||  v_pos_2),
	-- 'MI',(v_pos_10  ||  v_pos_11),
	-- 'ML', (v_pos_16  ||  v_pos_17),
	-- 'MP', (v_pos_1  ||  v_pos_2),
	-- 'M2', (v_pos_15  ||  v_pos_16),'  ')
	-- 
	-- 
	-- 
	-- 
	DECODE(
	    Type_Bureau,
	    'AI', (v_pos_11 || v_pos_12),
	    'AL', (v_pos_15 || v_pos_16),
	    'AN', (v_pos_12 || v_pos_13),
	    'AP', (v_pos_12 || v_pos_13),
	    'A2', (v_pos_8 || v_pos_9),
	    'A3', (v_pos_8 || v_pos_9),
	    'BB', (v_pos_20 || v_pos_21),
	    'BC', (v_pos_18 || v_pos_19),
	    'BE', (v_pos_4 || v_pos_5),
	    'BF', (v_pos_1 || v_pos_2),
	    'BP', (' ' || v_pos_2),
	    'BI', (v_pos_3 || v_pos_4),
	    'BL', (v_pos_3 || v_pos_4),
	    'BM', (v_pos_20 || v_pos_21),
	    'BT', (v_pos_11 || v_pos_12),
	    'B2', (v_pos_14 || v_pos_15),
	    'CF', (v_pos_8 || v_pos_9),
	    'CI', (v_pos_3 || v_pos_4),
	    'CN', (v_pos_1 || v_pos_2),
	    'CP', (v_pos_3 || v_pos_4),
	    'EI', (v_pos_2 || v_pos_3),
	    'EQ', (v_pos_8 || v_pos_9),
	    'FF', (v_pos_8 || v_pos_9),
	    'FI', (v_pos_1 || v_pos_2),
	    'FM', (v_pos_6 || v_pos_7),
	    'FO', (v_pos_8 || v_pos_9),
	    'FP', (v_pos_2 || v_pos_3),
	    'FT', (v_pos_4 || v_pos_5),
	    'GI', (v_pos_10 || v_pos_11),
	    'GL', (v_pos_20 || v_pos_21),
	    'GM', (v_pos_1 || v_pos_2),
	    'GP', (v_pos_8 || v_pos_9),
	    'GS', (v_pos_3 || v_pos_4),
	    'II', (v_pos_1 || v_pos_2),
	    'IM', (v_pos_1 || v_pos_2),
	    'MI', (v_pos_10 || v_pos_11),
	    'ML', (v_pos_16 || v_pos_17),
	    'MP', (v_pos_1 || v_pos_2),
	    'M2', (v_pos_15 || v_pos_16),
	    '  '
	) AS V_Policy_Type,
	V_Policy_Type AS Policy_Type,
	-- *INF*: SUBSTR(sar_class_code,1,3)
	SUBSTR(sar_class_code, 1, 3) AS v_sar_class_3,
	-- *INF*: DECODE(TRUE,
	-- IN (Type_Bureau,'BP','FP','BF','FT'),V_Policy_Type)
	DECODE(
	    TRUE,
	    Type_Bureau IN ('BP','FP','BF','FT'), V_Policy_Type
	) AS v_type_policy_45,
	-- *INF*: DECODE(TRUE,
	-- Type_Bureau='BP',v_pos_2,
	-- Type_Bureau='BF',v_pos_2,
	-- Type_Bureau='FP',' ',
	-- Type_Bureau='FT',' '  )
	DECODE(
	    TRUE,
	    Type_Bureau = 'BP', v_pos_2,
	    Type_Bureau = 'BF', v_pos_2,
	    Type_Bureau = 'FP', ' ',
	    Type_Bureau = 'FT', ' '
	) AS v_type_of_bond_6,
	-- *INF*: DECODE(TRUE,
	--  IN(Type_Bureau,'BP','BF','FP','FT'),v_sar_class_3  || v_type_policy_45 || v_type_of_bond_6,
	-- sar_class_code)
	DECODE(
	    TRUE,
	    Type_Bureau IN ('BP','BF','FP','FT'), v_sar_class_3 || v_type_policy_45 || v_type_of_bond_6,
	    sar_class_code
	) AS v_hold_sar_class_code,
	v_hold_sar_class_code AS sar_class_code_out
	FROM JNR_Reinsurance_Coverage_n_Transaction_Reserves
),
LKP_gtamTM08_stage_Reserves AS (
	SELECT
	coverage_code,
	major_peril
	FROM (
		SELECT gtam_tm08_stage.coverage_code as coverage_code, 
		RTRIM(gtam_tm08_stage.major_peril) as major_peril 
		FROM gtam_tm08_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY major_peril ORDER BY coverage_code DESC) = 1
),
EXP_Generate_Part_7_Reinsurance_Reserves AS (
	SELECT
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_ins_line AS reins_insurance_line,
	-- *INF*: IIF(ISNULL(reins_insurance_line) OR LENGTH(reins_insurance_line)=0,'NA',reins_insurance_line)
	IFF(
	    reins_insurance_line IS NULL OR LENGTH(reins_insurance_line) = 0, 'NA', reins_insurance_line
	) AS v_reins_insurance_line,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_loc_unit_num AS reins_location_number,
	-- *INF*: IIF(ISNULL(reins_location_number),'0000',LPAD(LTRIM(RTRIM(TO_CHAR(reins_location_number))),4,'0'))
	-- 
	IFF(
	    reins_location_number IS NULL, '0000',
	    LPAD(LTRIM(RTRIM(TO_CHAR(reins_location_number))), 4, '0')
	) AS v_reins_location_number,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_sub_loc_unit_num AS reins_sub_location_number,
	-- *INF*: IIF(ISNULL(reins_sub_location_number),'000',LPAD(LTRIM(RTRIM(TO_CHAR(reins_sub_location_number))),3,'0'))
	IFF(
	    reins_sub_location_number IS NULL, '000',
	    LPAD(LTRIM(RTRIM(TO_CHAR(reins_sub_location_number))), 3, '0')
	) AS v_reins_sub_location_number,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_risk_unit_grp AS reins_risk_unit_group,
	-- *INF*: IIF(ISNULL(reins_risk_unit_group),'000',TO_CHAR(reins_risk_unit_group))
	IFF(reins_risk_unit_group IS NULL, '000', TO_CHAR(reins_risk_unit_group)) AS v_reins_risk_unit_group,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_risk_unit_grp_seq_num AS reins_seq_rsk_unt_grp,
	-- *INF*: IIF(ISNULL(reins_seq_rsk_unt_grp),'000',LPAD(TO_CHAR(reins_seq_rsk_unt_grp),3,'0'))
	IFF(reins_seq_rsk_unt_grp IS NULL, '000', LPAD(TO_CHAR(reins_seq_rsk_unt_grp), 3, '0')) AS v_reins_seq_rsk_unt_grp,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_risk_unit AS reins_location,
	v_reins_insurance_line || '-'  || v_reins_location_number  || '-' || v_reins_sub_location_number  || ' - '  ||  v_reins_risk_unit_group  || '-'  ||  v_reins_seq_rsk_unt_grp  || '-'  || reins_location  ||  '000' AS Total_Reins_Key,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_risk_unit_seq_num AS reins_risk_sequence,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_section_code,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_co_num AS reins_company_no,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_eff_date AS reins_effective_date,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_exp_date AS reins_expiration_date,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_type,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_prcnt_loss_ceded,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_prcnt_facultative_commssn AS reins_percent_fac_comm,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.risk_unit_complete AS loss_unit,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.ins_line_out AS sar_insurance_line,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.loc_unit_num_out AS sar_location_x,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.sub_loc_unit_num_out AS sar_sub_location_x,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.risk_unit_grp_out AS sar_risk_unit_group,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.risk_unit AS sar_unit,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.risk_unit_seq_num_out AS sar_seq_rsk_unt_a,
	-- *INF*: TO_DECIMAL(sar_location_x)
	CAST(sar_location_x AS FLOAT) AS v_sar_location,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.PMS_Account_Date_To_Check,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.major_peril_code_Out AS loss_major_peril,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.pms_trans_code,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.rsn_amended_code_Out AS Reason_Amended,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.section_code_Out AS sar_section,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.trans_amt,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.trans_hist_amt,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.LM_Amount_OutStanding,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.LM_Amount_Paid_Losses,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.LM_Amount_Paid_Expenses,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.LM_Unpaid_Loss_Adj_Exp,
	-- *INF*: LM_Amount_OutStanding  *  reins_prcnt_loss_ceded
	-- ---IIF(IN(pms_trans_code,'90','92','95','97','98','99') AND pms_trans_code <> '95',
	-- ----(trans_amt * reins_prcnt_loss_ceded),0.0)
	LM_Amount_OutStanding * reins_prcnt_loss_ceded AS V_LM_Amount_Outstanding,
	V_LM_Amount_Outstanding AS LM_Reins_Amount_Outstanding,
	-- *INF*:  LM_Amount_Paid_Losses  *  reins_prcnt_loss_ceded
	-- 
	-- 
	-- ---IIF(NOT IN(pms_trans_code,'90','92','95','97','98','99') 
	--     ---   AND NOT IN(pms_trans_code,'71','72','73','74','75','76','77','78',   '79'),
	--        ----(trans_amt* reins_prcnt_loss_ceded),0.0)
	-- 
	-- 
	-- ---IIF(NOT IN(pms_trans_code,'71','72','73','74','75','76','77','78','79'),
	-- ----(loss_paid_or_resv_amt* reins_prcnt_loss_ceded),0.0)
	LM_Amount_Paid_Losses * reins_prcnt_loss_ceded AS V_LM_Amount_Paid_Losses,
	-- *INF*: V_LM_Amount_Paid_Losses
	-- 
	-- --- LM_Amount_Paid_Losses  *  reins_prcnt_loss_ceded
	-- 
	-- --IIF(IN(pms_trans_code,'82','84','89'), 
	-- --- -1 * V_LM_Amount_Paid_Losses, V_LM_Amount_Paid_Losses)
	-- 
	-- 
	V_LM_Amount_Paid_Losses AS LM_Reins_Amount_Paid_Losses,
	-- *INF*: LM_Amount_Paid_Expenses * reins_prcnt_loss_ceded 
	-- 
	-- --IIF(IN(pms_trans_code,'71','72','73','74','75','76','77','78','79'),
	-- ----(trans_amt*reins_prcnt_loss_ceded),0.0)
	LM_Amount_Paid_Expenses * reins_prcnt_loss_ceded AS V_LM_Amount_Paid_Expenses,
	-- *INF*: V_LM_Amount_Paid_Expenses
	-- 
	-- ---IIF(pms_trans_code = '75' ,-1 * V_LM_Amount_Paid_Expenses,
	-- ---V_LM_Amount_Paid_Expenses)
	-- 
	-- ---- R21EX,R22EX,R23EX,R24EX converts into 75 when it backfeeds along with amount field being multiplied with -ve sign.
	-- 
	-- 
	-- 
	-- --
	V_LM_Amount_Paid_Expenses AS LM_Reins_Amount_Paid_Expenses,
	-- *INF*: LM_Unpaid_Loss_Adj_Exp  *  reins_prcnt_loss_ceded 
	-- 
	-- 
	-- ---IIF(IN(pms_trans_code,'90','92','95','97','98','99') AND pms_trans_code = '95',
	-- ---(trans_amt * reins_prcnt_loss_ceded),0.0)
	LM_Unpaid_Loss_Adj_Exp * reins_prcnt_loss_ceded AS V_LM_EOM_Unpaid_Loss_Adj_Exp,
	V_LM_EOM_Unpaid_Loss_Adj_Exp AS LM_Reins_EOM_Unpaid_Loss_Adj_Exp,
	-- *INF*: --IIF(IN(pif_line_business,'CPP','BO','BOP','SMP','GL','CA','CF','CG','CR','IM','BM'),'MORECHECK','NOCHECK')
	'' AS Check_Type,
	-- *INF*: IIF((loss_date<=reins_expiration_date) AND (reins_effective_date <=loss_date) AND 
	-- reins_prcnt_loss_ceded <> 0.0 
	--          ,'VALID','INVALID')
	-- 
	-- 
	IFF(
	    (loss_date <= reins_expiration_date)
	    and (reins_effective_date <= loss_date)
	    and reins_prcnt_loss_ceded <> 0.0,
	    'VALID',
	    'INVALID'
	) AS v_reins_validity,
	-- *INF*: DECODE(v_reins_validity,'INVALID','NOREINSURANCE',
	--                                       'VALID',
	-- DECODE(TRUE,
	-- v_reins_insurance_line = 'GL' and v_reins_location_number <> sar_location_x,'NOREINSURANCE',
	-- reins_section_code =  sar_section AND reins_location=loss_unit_3pos_out OR reins_location = 'N/A', 'REINSURANCE',
	-- (sar_section = '2' AND IN(Reason_Amended,'COL','CWO')) AND  (reins_location = 'N/A'  OR reins_location=loss_unit_3pos_out),
	-- 'REINSURANCE')
	-- )
	-- 
	-- ----- 05/12/2011 Changed the below logic to above logic to insurance_line = 'GL' and based on location_num
	-- 
	-- --DECODE(v_reins_validity,'INVALID','NOREINSURANCE',
	-- --                                      'VALID',
	-- --IIF(reins_section_code =  sar_section AND reins_location=loss_unit_3pos_out OR IS_SPACES(reins_location), 'REINSURANCE',
	-- --IIF((sar_section = '2' AND IN(Reason_Amended,'COL','CWO')) AND
	-- -- (IS_SPACES(reins_location) OR reins_location=loss_unit_3pos_out),
	-- --'REINSURANCE','NOREINSURANCE'))
	-- --)
	-- 
	-- --- Changed the logic 
	-- 
	-- --DECODE(v_reins_validity,'INVALID','NOREINSURANCE',
	-- --'VALID',
	-- --IIF(ISNULL(reins_location_number),
	-- --IIF(reins_location=loss_unit OR IS_SPACES(reins_location) OR reins_location = '0','REINSURANCE','NOREINSURANCE'),
	-- --IIF(
	-- --(sar_section = '2' AND IN(Reason_Amended,'COL','CWO')) AND (IS_SPACES(reins_location) OR reins_location=loss_unit)
	-- --,
	-- --'REINSURANCE','NOREINSURANCE'))
	-- --)
	DECODE(
	    v_reins_validity,
	    'INVALID', 'NOREINSURANCE',
	    'VALID', DECODE(
	        TRUE,
	        v_reins_insurance_line = 'GL' and v_reins_location_number <> sar_location_x, 'NOREINSURANCE',
	        reins_section_code = sar_section AND reins_location = loss_unit_3pos_out OR reins_location = 'N/A', 'REINSURANCE',
	        (sar_section = '2' AND Reason_Amended IN ('COL','CWO')) AND (reins_location = 'N/A' OR reins_location = loss_unit_3pos_out), 'REINSURANCE'
	    )
	) AS v_reinsurance_flag,
	v_reinsurance_flag AS reinsurance_flag,
	-- *INF*: DECODE(v_reinsurance_flag,'NOREINSURANCE','NOGENERATE',
	--                                                              'REINSURANCE',
	--                  IIF(reins_company_no = '0058' and IN(pms_trans_code ,'73','75','76','77'),'NOGENERATE','GENERATE'))
	-- 
	-- --- Added the Reinsurance_Company No logic on 11/11/2010
	-- ----IIF(v_INSURANCE_LINE_GL = 'VALID','GENERATE','NOGENERATE')
	DECODE(
	    v_reinsurance_flag,
	    'NOREINSURANCE', 'NOGENERATE',
	    'REINSURANCE', IFF(
	        reins_company_no = '0058' and pms_trans_code IN ('73','75','76','77'), 'NOGENERATE',
	        'GENERATE'
	    )
	) AS v_Reins_generate_flag,
	v_Reins_generate_flag AS V_Generate_flag,
	-- *INF*: IIF(v_Reins_generate_flag = 'GENERATE' ,
	-- IIF(V_LM_Amount_Outstanding = 0.0 AND 
	--        V_LM_Amount_Paid_Losses= 0.0 AND
	-- V_LM_Amount_Paid_Expenses = 0.0 AND
	-- V_LM_EOM_Unpaid_Loss_Adj_Exp = 0.0,'FALSE','TRUE'),'FALSE')
	IFF(
	    v_Reins_generate_flag = 'GENERATE',
	    IFF(
	        V_LM_Amount_Outstanding = 0.0
	        and V_LM_Amount_Paid_Losses = 0.0
	        and V_LM_Amount_Paid_Expenses = 0.0
	        and V_LM_EOM_Unpaid_Loss_Adj_Exp = 0.0,
	        'FALSE',
	        'TRUE'
	    ),
	    'FALSE'
	) AS Generate_flag,
	@{pipeline().parameters.ACCOUNT_DATE} AS Account_Entered_Date,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.sar_id AS loss_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.Claim_loss_date AS loss_date,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.risk_unit_3pos AS loss_unit_3pos_out,
	sar_insurance_line AS sar_insurance_line_calculated,
	-- *INF*: IIF(sar_insurance_line='GL' and sar_location_x  != '0000','0000',sar_location_x)
	IFF(sar_insurance_line = 'GL' and sar_location_x != '0000', '0000', sar_location_x) AS sar_location_calculated,
	-- *INF*: DECODE(TRUE,
	-- IN(sar_insurance_line,'GL','LI'),'000',
	-- sar_insurance_line != 'GA' OR (sar_insurance_line = 'GA' AND loss_id = '51'),sar_sub_location_x) 
	DECODE(
	    TRUE,
	    sar_insurance_line IN ('GL','LI'), '000',
	    sar_insurance_line != 'GA' OR (sar_insurance_line = 'GA' AND loss_id = '51'), sar_sub_location_x
	) AS sar_sub_location_calculated,
	-- *INF*: --IIF(sar_insurance_line = 'CR','000',sar_risk_unit_group)
	'' AS sar_risk_unit_group_calculated,
	-- *INF*: ---IIF(IN(sar_insurance_line,'CR','CF'),'000',sar_class_code_grp_x || sar_class_code_mem_x)
	'' AS sar_risk_unit_group_seq_calculated,
	-- *INF*: ---IIF(sar_insurance_line = 'GA',sar_unit  || '000',sar_unit || sar_risk_unit_continued)
	'' AS sar_unit_calculated,
	-- *INF*: --sar_insurance_line_calculated || '-'  || sar_location_calculated  || '-' || sar_sub_location_calculated  || ' - '  ||  sar_risk_unit_group_calculated  || '-'  ||  sar_risk_unit_group_seq_calculated  || '-'  || sar_unit_calculated
	'' AS Total_Sar_Key,
	-- *INF*: --IIF(Total_Reins_Key  = Total_Sar_Key , 'TRUE','FALSE')
	'' AS Key_Check,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.Policy_Symbol,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.Policy_Number,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.Policy_Module,
	EXP_Transform_Statistical_Codes_Reserves.Type_Bureau,
	EXP_Transform_Statistical_Codes_Reserves.Formatted_Stat_Codes,
	EXP_Transform_Statistical_Codes_Reserves.Formatted_Stat_Codes_26_34,
	EXP_Transform_Statistical_Codes_Reserves.Formatted_Stat_Codes_34_38,
	EXP_Transform_Statistical_Codes_Reserves.Policy_Type,
	-- *INF*: IIF(ISNULL(Policy_Type),'N/A',Policy_Type)
	IFF(Policy_Type IS NULL, 'N/A', Policy_Type) AS Policy_Type_out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.risk_state_prov_code_Out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.risk_zip_code_Out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.terr_code_Out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.tax_loc_Out,
	EXP_Transform_Statistical_Codes_Reserves.sar_class_code_out AS Class_Code,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.exposure_Out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.sub_line_code_Out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.source_sar_asl_Out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.source_sar_prdct_line_Out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.source_sar_sp_use_code,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.reins_cov_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.claim_rep_ak_id_H,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.claim_rep_ak_id_E,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.claim_trans_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.claim_trans_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.agency_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.contract_cust_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.pol_ak_id_Out1,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.claim_occurrence_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.claim_party_occurrence_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.claim_party_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.claim_case_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.claimant_cov_det_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.wc_claimant_det_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.claim_pay_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.cov_ak_id_Out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.temp_pol_trans_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.new_claim_count,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.Statistical_Line AS statistical_line,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.inception_Date,
	-- *INF*: SUBSTR(source_sar_prdct_line_Out,1,2)
	SUBSTR(source_sar_prdct_line_Out, 1, 2) AS source_stat_brkdown_line,
	'N/A' AS Default_NA,
	-1 AS Default_ID,
	LKP_gtamTM08_stage_Reserves.coverage_code,
	-- *INF*: IIF(ISNULL(coverage_code),'N/A',coverage_code)
	IFF(coverage_code IS NULL, 'N/A', coverage_code) AS coverage_code_out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.Loss_Master_Run_Date AS loss_master_run_date,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.LM_Variation_Code,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.trans_date,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.pms_acct_entered_date,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.trans_offset_onset_ind,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.StatisticalCoverageAKID,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.RatingCoverageAKID,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.financial_type_code,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.trans_code,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.PolicyCoverageAKID,
	-- *INF*: IIF(ISNULL(PolicyCoverageAKID),-1,PolicyCoverageAKID)
	IFF(PolicyCoverageAKID IS NULL, - 1, PolicyCoverageAKID) AS PolicyCoverageAKID_Out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.RiskLocationAKID,
	-- *INF*: IIF(ISNULL(RiskLocationAKID),-1,RiskLocationAKID)
	IFF(RiskLocationAKID IS NULL, - 1, RiskLocationAKID) AS RiskLocationAKID_Out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.PremiumTransactionAKID,
	-- *INF*: IIF(ISNULL(PremiumTransactionAKID),-1,PremiumTransactionAKID)
	IFF(PremiumTransactionAKID IS NULL, - 1, PremiumTransactionAKID) AS PremiumTransactionAKID_Out,
	JNR_Reinsurance_Coverage_n_Transaction_Reserves.BureauStatisticalCodeAKID,
	-- *INF*: IIF(ISNULL(BureauStatisticalCodeAKID),-1,BureauStatisticalCodeAKID)
	IFF(BureauStatisticalCodeAKID IS NULL, - 1, BureauStatisticalCodeAKID) AS BureauStatisticalCodeAKID_Out
	FROM EXP_Transform_Statistical_Codes_Reserves
	 -- Manually join with JNR_Reinsurance_Coverage_n_Transaction_Reserves
	LEFT JOIN LKP_gtamTM08_stage_Reserves
	ON LKP_gtamTM08_stage_Reserves.major_peril = JNR_Reinsurance_Coverage_n_Transaction_Reserves.major_peril_code_Out
),
FIL_Reinsurance_Rows_Reserves AS (
	SELECT
	Policy_Symbol AS pif_symbol, 
	Policy_Number AS pif_policy_number, 
	Policy_Module AS pif_module, 
	pms_trans_code, 
	Generate_flag, 
	Account_Entered_Date, 
	claim_trans_id AS claim_trans_pk_id, 
	claim_trans_ak_id, 
	Default_ID AS claim_reins_trans_pk_id, 
	Default_ID AS claim_reins_trans_ak_id, 
	agency_ak_id, 
	contract_cust_ak_id, 
	pol_ak_id_Out1 AS pol_ak_id, 
	claim_occurrence_ak_id, 
	claim_rep_ak_id_H AS claim_primary_rep_ak_id, 
	claim_rep_ak_id_E AS claim_examiner_ak_id, 
	claim_party_occurrence_ak_id, 
	claim_party_ak_id, 
	claim_case_ak_id, 
	claimant_cov_det_ak_id, 
	wc_claimant_det_ak_id, 
	claim_pay_ak_id, 
	cov_ak_id_Out AS cov_ak_id, 
	temp_pol_trans_ak_id, 
	reins_cov_ak_id, 
	LM_Variation_Code AS variation_code, 
	Policy_Type_out AS pol_type, 
	inception_Date AS incptn_date, 
	loss_master_run_date, 
	new_claim_count, 
	trans_amt, 
	trans_hist_amt, 
	LM_Reins_Amount_Outstanding AS outstanding_amt, 
	LM_Reins_Amount_Paid_Losses AS paid_loss_amt, 
	LM_Reins_Amount_Paid_Expenses AS paid_exp_amt, 
	LM_Reins_EOM_Unpaid_Loss_Adj_Exp AS eom_unpaid_loss_adjust_exp, 
	trans_hist_amt AS orig_reserve, 
	Default_NA AS auto_reins_facility, 
	source_stat_brkdown_line AS statistical_brkdwn_line, 
	Formatted_Stat_Codes AS statistical_code1, 
	Formatted_Stat_Codes_26_34 AS statistical_code2, 
	Formatted_Stat_Codes_34_38 AS statistical_code3, 
	statistical_line, 
	coverage_code_out AS loss_master_cov_code, 
	risk_state_prov_code_Out AS risk_state_prov_code, 
	risk_zip_code_Out AS risk_zip_code, 
	terr_code_Out AS terr_code, 
	tax_loc_Out AS tax_loc, 
	Class_Code AS class_code, 
	exposure_Out AS exposure, 
	sub_line_code_Out AS sub_line_code, 
	source_sar_asl_Out AS source_sar_asl, 
	source_sar_prdct_line_Out AS source_sar_prdct_line, 
	source_sar_sp_use_code, 
	trans_date, 
	pms_acct_entered_date, 
	trans_offset_onset_ind, 
	StatisticalCoverageAKID, 
	RatingCoverageAKID, 
	financial_type_code, 
	trans_code, 
	PolicyCoverageAKID_Out AS PolicyCoverageAKID, 
	RiskLocationAKID_Out AS RiskLocationAKID, 
	PremiumTransactionAKID_Out AS PremiumTransactionAKID, 
	BureauStatisticalCodeAKID_Out AS BureauStatisticalCodeAKID
	FROM EXP_Generate_Part_7_Reinsurance_Reserves
	WHERE Generate_flag='TRUE'
),
SEQ_Loss_Master_Calculation_AK_ID AS (
	CREATE SEQUENCE SEQ_Loss_Master_Calculation_AK_ID
	START = 0
	INCREMENT = 1;
),
EXP_Determine_AK_ID_Reserves AS (
	SELECT
	'1' AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 00:00:01','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 00:00:01', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	SEQ_Loss_Master_Calculation_AK_ID.NEXTVAL AS loss_master_calculation_ak_id,
	claim_trans_pk_id,
	claim_trans_ak_id,
	claim_reins_trans_pk_id,
	claim_reins_trans_ak_id,
	agency_ak_id,
	contract_cust_ak_id,
	pol_ak_id,
	claim_occurrence_ak_id,
	claim_primary_rep_ak_id,
	claim_examiner_ak_id,
	claim_party_occurrence_ak_id,
	claim_party_ak_id,
	claim_case_ak_id,
	claimant_cov_det_ak_id,
	wc_claimant_det_ak_id,
	claim_pay_ak_id,
	cov_ak_id,
	temp_pol_trans_ak_id,
	reins_cov_ak_id,
	'C' AS trans_kind_code,
	variation_code,
	pol_type,
	incptn_date,
	loss_master_run_date,
	new_claim_count,
	trans_amt,
	trans_hist_amt,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	eom_unpaid_loss_adjust_exp,
	orig_reserve,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code1,
	-- *INF*: IIF(ISNULL(statistical_code1),'N/A',statistical_code1)
	IFF(statistical_code1 IS NULL, 'N/A', statistical_code1) AS statistical_code1_Out,
	statistical_code2,
	-- *INF*: IIF(ISNULL(statistical_code2),'N/A',statistical_code2)
	IFF(statistical_code2 IS NULL, 'N/A', statistical_code2) AS statistical_code2_Out,
	statistical_code3,
	-- *INF*: IIF(ISNULL(statistical_code3),'N/A',statistical_code3)
	IFF(statistical_code3 IS NULL, 'N/A', statistical_code3) AS statistical_code3_Out,
	statistical_line,
	loss_master_cov_code,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	pms_trans_code,
	trans_date,
	pms_acct_entered_date,
	trans_offset_onset_ind,
	StatisticalCoverageAKID,
	-1 AS o_Default,
	RatingCoverageAKID AS RatingCoverageAKId,
	financial_type_code,
	trans_code,
	PolicyCoverageAKID,
	RiskLocationAKID,
	PremiumTransactionAKID,
	BureauStatisticalCodeAKID
	FROM FIL_Reinsurance_Rows_Reserves
),
loss_master_calculation_Reserves AS (
	INSERT INTO loss_master_calculation
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, loss_master_calculation_ak_id, claim_trans_pk_id, claim_trans_ak_id, claim_reins_trans_pk_id, claim_reins_trans_ak_id, agency_ak_id, contract_cust_ak_id, pol_ak_id, claim_occurrence_ak_id, claim_primary_rep_ak_id, claim_examiner_ak_id, claim_party_occurrence_ak_id, claim_party_ak_id, claim_case_ak_id, claimant_cov_det_ak_id, wc_claimant_det_ak_id, claim_pay_ak_id, cov_ak_id, temp_pol_trans_ak_id, reins_cov_ak_id, trans_kind_code, variation_code, pol_type, incptn_date, loss_master_run_date, new_claim_count, outstanding_amt, paid_loss_amt, paid_exp_amt, eom_unpaid_loss_adjust_exp, orig_reserve, auto_reins_facility, statistical_brkdwn_line, statistical_code1, statistical_code2, statistical_code3, statistical_line, loss_master_cov_code, risk_state_prov_code, risk_zip_code, terr_code, tax_loc, class_code, exposure, sub_line_code, source_sar_asl, source_sar_prdct_line, source_sar_sp_use_code, pms_trans_code, trans_date, pms_acct_entered_date, trans_offset_onset_ind, claim_trans_amt, claim_trans_hist_amt, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, PremiumTransactionAKID, BureauStatisticalCodeAKID, RatingCoverageAKId, FinancialTypeCode, TransactionCode)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	LOSS_MASTER_CALCULATION_AK_ID, 
	CLAIM_TRANS_PK_ID, 
	CLAIM_TRANS_AK_ID, 
	CLAIM_REINS_TRANS_PK_ID, 
	CLAIM_REINS_TRANS_AK_ID, 
	AGENCY_AK_ID, 
	CONTRACT_CUST_AK_ID, 
	POL_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_PRIMARY_REP_AK_ID, 
	CLAIM_EXAMINER_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_AK_ID, 
	CLAIM_CASE_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	WC_CLAIMANT_DET_AK_ID, 
	CLAIM_PAY_AK_ID, 
	COV_AK_ID, 
	TEMP_POL_TRANS_AK_ID, 
	REINS_COV_AK_ID, 
	TRANS_KIND_CODE, 
	VARIATION_CODE, 
	POL_TYPE, 
	INCPTN_DATE, 
	LOSS_MASTER_RUN_DATE, 
	NEW_CLAIM_COUNT, 
	OUTSTANDING_AMT, 
	PAID_LOSS_AMT, 
	PAID_EXP_AMT, 
	EOM_UNPAID_LOSS_ADJUST_EXP, 
	ORIG_RESERVE, 
	AUTO_REINS_FACILITY, 
	STATISTICAL_BRKDWN_LINE, 
	statistical_code1_Out AS STATISTICAL_CODE1, 
	statistical_code2_Out AS STATISTICAL_CODE2, 
	statistical_code3_Out AS STATISTICAL_CODE3, 
	STATISTICAL_LINE, 
	LOSS_MASTER_COV_CODE, 
	RISK_STATE_PROV_CODE, 
	RISK_ZIP_CODE, 
	TERR_CODE, 
	TAX_LOC, 
	CLASS_CODE, 
	EXPOSURE, 
	SUB_LINE_CODE, 
	SOURCE_SAR_ASL, 
	SOURCE_SAR_PRDCT_LINE, 
	SOURCE_SAR_SP_USE_CODE, 
	PMS_TRANS_CODE, 
	TRANS_DATE, 
	PMS_ACCT_ENTERED_DATE, 
	TRANS_OFFSET_ONSET_IND, 
	trans_amt AS CLAIM_TRANS_AMT, 
	trans_hist_amt AS CLAIM_TRANS_HIST_AMT, 
	RISKLOCATIONAKID, 
	POLICYCOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	PREMIUMTRANSACTIONAKID, 
	BUREAUSTATISTICALCODEAKID, 
	RATINGCOVERAGEAKID, 
	financial_type_code AS FINANCIALTYPECODE, 
	trans_code AS TRANSACTIONCODE
	FROM EXP_Determine_AK_ID_Reserves
),
SQ_claim_transaction AS (
	SELECT 
	       CT.claim_trans_id,
	       CT.claim_trans_ak_id,
	       CT.claimant_cov_det_ak_id,
	       CT.claim_pay_ak_id, 
	       CT.cause_of_loss,
	       CT.reserve_ctgry,
	       CT.type_disability,
	       CT.sar_id,
	       CT.financial_type_code,
	       CT.s3p_trans_code,
	       CT.pms_trans_code,
	       CT.trans_code,
	       CT.trans_date,
	       CT.pms_acct_entered_date,
	       CT.trans_base_type_code,
	       CT.trans_ctgry_code,
	       CT.trans_amt,
	       CT.trans_hist_amt,
	       CT.draft_num,
	       CT.source_sys_id,
	       CT.trans_offset_onset_ind
	FROM   
	       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_transaction CT
	WHERE CT.err_flag > = 0
	and NOT (CT.pms_trans_code in ('90','92','95','97','98','99') and CT.source_sys_id='exceed')
	and not CT.financial_type_code + CT.s3p_trans_code in ('E66','E65','B65','B66','R65','R66','S65','S66')
	and CT.source_sys_id='EXCEED'
	and CT.trans_date <= @{pipeline().parameters.DATESQL}
	--debug
	--and CT.claimant_cov_det_ak_id=-10
	
	UNION ALL
	
	SELECT 
		CT2.claim_trans_id,
	       CT2.claim_trans_ak_id,
	       CT2.claimant_cov_det_ak_id,
	       CT2.claim_pay_ak_id, 
	       CT2.cause_of_loss,
	       CT2.reserve_ctgry,
	       CT2.type_disability,
	       CT2.sar_id,
	       CT2.financial_type_code,
	       CT2.s3p_trans_code,
	       CT2.pms_trans_code,
	       CT2.trans_code,
	       CT2.trans_date,
	       CT2.pms_acct_entered_date,
	       CT2.trans_base_type_code,
	       CT2.trans_ctgry_code,
	       CT2.trans_amt,
	       CT2.trans_hist_amt,
	       CT2.draft_num,
	       CT2.source_sys_id,
	       CT2.trans_offset_onset_ind 
	FROM vw_claim_transaction CT2
	WHERE CT2.err_flag > = 0
	and  UPPER(CT2.source_sys_id)='PMS'
	and not CT2.financial_type_code + CT2.s3p_trans_code in ('E66','E65','B65','B66','R65','R66','S65','S66')
	and CT2.trans_date <= @{pipeline().parameters.DATESQL}
	--debug
	--and CT2.claimant_cov_det_ak_id=-10
),
EXP_Default AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	financial_type_code,
	s3p_trans_code,
	pms_trans_code,
	trans_code,
	trans_date,
	pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	-- *INF*: financial_type_code || s3p_trans_code  ||  IIF(trans_ctgry_code = 'N/A','',trans_ctgry_code)
	financial_type_code || s3p_trans_code || IFF(trans_ctgry_code = 'N/A', '', trans_ctgry_code) AS v_Exceed_Trans_Code,
	v_Exceed_Trans_Code AS Exceed_Trans_Code,
	trans_amt,
	trans_hist_amt,
	draft_num,
	source_sys_id,
	trans_offset_onset_ind
	FROM SQ_claim_transaction
),
EXP_Loss_Master_Offset_Onset AS (
	SELECT
	claim_trans_id,
	claim_trans_ak_id,
	claimant_cov_det_ak_id,
	claim_pay_ak_id,
	cause_of_loss,
	reserve_ctgry,
	type_disability,
	sar_id,
	financial_type_code,
	s3p_trans_code,
	pms_trans_code,
	trans_code,
	trans_date,
	pms_acct_entered_date,
	trans_base_type_code,
	trans_ctgry_code,
	Exceed_Trans_Code,
	trans_amt,
	trans_hist_amt,
	draft_num,
	source_sys_id,
	trans_offset_onset_ind,
	-- *INF*: ADD_TO_DATE(SYSDATE,'MM',-1)
	DATEADD(MONTH,- 1,CURRENT_TIMESTAMP) AS V_Last_Month_Date,
	-- *INF*: LAST_DAY(V_Last_Month_Date)
	LAST_DAY(V_Last_Month_Date) AS V_Last_Month_Last_Day_Date,
	-- *INF*: GET_DATE_PART(V_Last_Month_Last_Day_Date,'YYYY')
	DATE_PART(V_Last_Month_Last_Day_Date, 'YYYY') AS V_Account_Date_YYYY,
	-- *INF*: GET_DATE_PART(V_Last_Month_Last_Day_Date,'MM')
	DATE_PART(V_Last_Month_Last_Day_Date, 'MM') AS V_Account_Date_MM,
	-- *INF*: V_Account_Date_YYYY || LPAD(V_Account_Date_MM,2,'0')
	V_Account_Date_YYYY || LPAD(V_Account_Date_MM, 2, '0') AS V_Account_Date,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART( V_Last_Month_Last_Day_Date, 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	-- 
	-- 
	-- --- Changing the date to have 00:00:00 in the timestamp part
	DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date)),DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date)),DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date))) AS v_Loss_Master_Run_Date,
	-- *INF*: IIF(trans_offset_onset_ind = 'O', pms_acct_entered_date, ADD_TO_DATE(v_Loss_Master_Run_Date,'dd',1))
	-- 
	-- --- Above logic is very important for Loss Master generation for EDW. We had to use above so that for EXCEED Offset Transactions we can get the attributes from Dim tables as that day so we are using pms_acct_entered_date. And for other transactions we use loss_master_run_date.
	IFF(
	    trans_offset_onset_ind = 'O', pms_acct_entered_date, DATEADD(dd,1,v_Loss_Master_Run_Date)
	) AS Loss_Master_Run_Date
	FROM EXP_Default
),
mplt_LM_Policy_n_Claim_Attributes AS (WITH
	INPUT AS (
		
	),
	LKP_Claimant_Coverage_Detail AS (
		SELECT
		claim_party_occurrence_ak_id,
		loc_unit_num,
		sub_loc_unit_num,
		ins_line,
		risk_unit_grp,
		risk_unit_grp_seq_num,
		risk_unit,
		risk_unit_seq_num,
		major_peril_code,
		major_peril_seq,
		pms_loss_disability,
		reserve_ctgry,
		cause_of_loss,
		pms_type_exposure,
		pms_type_bureau_code,
		risk_type_ind,
		PolicySourceID,
		ClassCode,
		SublineCode,
		RatingCoverageAKId,
		StatisticalCoverageAKID,
		PolicyCoverageAKID,
		RiskLocationAKID,
		PremiumTransactionAKID,
		BureauStatisticalCodeAKID,
		RiskTerritory,
		StateProvinceCode,
		ZipPostalCode,
		TaxLocation,
		Exposure,
		BureauStatisticalcode1_15,
		BureauSpecialUseCode,
		PMSAnnualStatementLine,
		RatingDateIndicator,
		BureauStatisticalUserLine,
		AuditReinstatementIndicator,
		claimant_cov_det_ak_id,
		eff_from_date,
		eff_to_date
		FROM (
			SELECT Ab.claim_party_occurrence_ak_id AS Claim_party_occurrence_ak_id,
			       Ab.loc_unit_num                 AS Loc_unit_num,
			       Ab.sub_loc_unit_num             AS Sub_loc_unit_num,
			       Ab.ins_line                     AS Ins_line,
			       Ab.risk_unit_grp                AS Risk_unit_grp,
			       Ab.risk_unit_grp_seq_num        AS Risk_unit_grp_seq_num,
			       Ab.risk_unit                    AS Risk_unit,
			       Ab.risk_unit_seq_num            AS Risk_unit_seq_num,
			       Ab.major_peril_code             AS Major_peril_code,
			       Ab.major_peril_seq              AS Major_peril_seq,
			       Ab.pms_loss_disability          AS Pms_loss_disability,
			       Ab.reserve_ctgry                AS Reserve_ctgry,
			       Ab.cause_of_loss                AS Cause_of_loss,
			       Ab.pms_type_exposure            AS Pms_type_exposure,
			       Ab.pms_type_bureau_code         AS Pms_type_bureau_code,
			       Ab.risk_type_ind                AS Risk_type_ind,
			       Ab.PolicySourceID               AS Policysourceid,
			       isnull(Sc.ClassCode,'N/A')                    AS Classcode,
			       isnull(Sc.SublineCode,'N/A')                  AS Sublinecode,
			       Ab.RatingCoverageAKId           AS Ratingcoverageakid,
			       Ab.StatisticalCoverageAKID      AS Statisticalcoverageakid,
			       isnull(Pc.PolicyCoverageAKID,-1)           AS Policycoverageakid,
			       isnull(Rl.RiskLocationAKID,-1)             AS Risklocationakid,
			       isnull(Pt.PremiumTransactionAKID,-1)       AS Premiumtransactionakid,
			       isnull(Bsc.BureauStatisticalCodeAKID,-1)   AS Bureaustatisticalcodeakid,
			       isnull(Rl.RiskTerritory, 'N/A')              AS Riskterritory,
			       isnull(Rl.StateProvinceCode , 'N/A')           AS Stateprovincecode,
			       isnull(Rl.ZipPostalCode,'N/A')                AS Zippostalcode,
			       isnull(Rl.TaxLocation,'N/A')                  AS Taxlocation,
			       isnull(Pt.Exposure,0)                     AS Exposure,
			       isnull(BureauCode1 + BureauCode2 + BureauCode3 + BureauCode4 + BureauCode5 + 
			       BureauCode6 + BureauCode7 + BureauCode8 + BureauCode9 + BureauCode10 + 
			       BureauCode11 + BureauCode12 + BureauCode13 + BureauCode14 + BureauCode15, 'N/A') AS Bureaustatisticalcode1_15,
			       isnull(BureauSpecialUseCode, 'N/A')            AS BureauSpecialUseCode,
			       isnull(PMSAnnualStatementLine, 'N/A')		   AS PMSAnnualStatementLine,
			       isnull(RatingDateIndicator, 'N/A')             AS RatingDateIndicator,
			       isnull(BureauStatisticalUserLine, 'N/A')       AS BureauStatisticalUserLine,
			       isnull(AuditReinstatementIndicator, 'N/A')     AS AuditReinstatementIndicator,
			       Ab.claimant_cov_det_ak_id       AS Claimant_cov_det_ak_id,
			       Ab.eff_from_date                AS Eff_from_date,
			       Ab.eff_to_date                  AS Eff_to_date
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL Ab
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.STATISTICALCOVERAGE Sc
			on Ab.StatisticalCoverageAKID = Sc.StatisticalCoverageAKID
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.POLICYCOVERAGE Pc
			on Sc.PolicyCoverageAKID = Pc.PolicyCoverageAKID
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.RISKLOCATION Rl
			on Pc.RiskLocationAKID = Rl.RiskLocationAKID
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.PREMIUMTRANSACTION Pt
			on Sc.StatisticalCoverageAKID = Pt.StatisticalCoverageAKID
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.BUREAUSTATISTICALCODE Bsc
			on Pt.PremiumTransactionAKID = Bsc.PremiumTransactionAKID
			WHERE -- Ab.audit_id > 0 AND -- (Removing the Audit_ID condition as part of Ticket #663312)
				  Ab.PolicySourceID NOT IN ( 'PDC', 'DUC' )  ---- To pull data from coverages of PMS policies only
			       
			UNION
			
			SELECT Ab.claim_party_occurrence_ak_id AS Claim_party_occurrence_ak_id,
			       Ab.loc_unit_num                 AS Loc_unit_num,
			       Ab.sub_loc_unit_num             AS Sub_loc_unit_num,
			       Ab.ins_line                     AS Ins_line,
			       Ab.risk_unit_grp                AS Risk_unit_grp,
			       Ab.risk_unit_grp_seq_num        AS Risk_unit_grp_seq_num,
			       Ab.risk_unit                    AS Risk_unit,
			       Ab.risk_unit_seq_num            AS Risk_unit_seq_num,
			       Ab.major_peril_code             AS Major_peril_code,
			       Ab.major_peril_seq              AS Major_peril_seq,
			       Ab.pms_loss_disability          AS Pms_loss_disability,
			       Ab.reserve_ctgry                AS Reserve_ctgry,
			       Ab.cause_of_loss                AS Cause_of_loss,
			       Ab.pms_type_exposure            AS Pms_type_exposure,
			       Ab.pms_type_bureau_code         AS Pms_type_bureau_code,
			       Ab.risk_type_ind                AS Risk_type_ind,
			       Ab.PolicySourceID               AS Policysourceid,
			       Rc.Classcode                  AS Classcode,
			       Ab.SublineCode                  AS Sublinecode,
			       Ab.RatingCoverageAKId           AS Ratingcoverageakid,
			       Ab.StatisticalCoverageAKID      AS Statisticalcoverageakid,
			       isnull(Pc.PolicyCoverageAKID,-1)           AS Policycoverageakid,
			       isnull(Rl.RiskLocationAKID,-1)             AS Risklocationakid,
			       isnull(Pt.PremiumTransactionAKID,-1)       AS Premiumtransactionakid,
			       -1                              AS Bureaustatisticalcodeakid,
			       isnull(Rl.RiskTerritory, 'N/A')                AS Riskterritory,
			       isnull(Rl.StateProvinceCode, 'N/A')            AS Stateprovincecode,
			       isnull(Rl.ZipPostalCode, 'N/A')                AS Zippostalcode,
			       isnull(Rl.TaxLocation,' N/A')                  AS Taxlocation,
			       isnull(Rc.Exposure, 0)                     AS Exposure,
			       'N/A'                           AS Bureaustatisticalcode1_15,
			       'N/A'                           AS BureauSpecialUseCode,
			       'N/A'                           AS PMSAnnualStatementLine,
			       'N/A'                           AS RatingDateIndicator,
			       'N/A'                           AS BureauStatisticalUserLine,
			       'N/A'                           AS AuditReinstatementIndicator,
			       Ab.claimant_cov_det_ak_id       AS Claimant_cov_det_ak_id,
			       Ab.eff_from_date                AS Eff_from_date,
			       Ab.eff_to_date                  AS Eff_to_date
			FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIMANT_COVERAGE_DETAIL Ab
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.RatingCoverage Rc
			on Ab.RatingCoverageAKId = Rc.RatingCoverageAKID 
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.POLICYCOVERAGE Pc
			on Rc.PolicyCoverageAKID = Pc.PolicyCoverageAKID 
			and Pc.CurrentSnapshotFlag=1
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.RISKLOCATION Rl
			on Pc.RiskLocationAKID = Rl.RiskLocationAKID 
			and Rl.CurrentSnapshotFlag=1
			left join @{pipeline().parameters.TARGET_TABLE_OWNER}.PREMIUMTRANSACTION Pt
			on Rc.RatingCoverageAKID = Pt.RatingCoverageAKID 
			and Pt.EffectiveDate=Rc.EffectiveDate
			and Pt.CurrentSnapshotFlag=1
			WHERE  -- Ab.audit_id > 0 AND -- -- (Removing the Audit_ID condition as part of Ticket #663312)
				   Ab.PolicySourceID IN ( 'PDC', 'DUC' )  ---- To pull data for coverages of DuckCreek policies only
			ORDER BY Ab.claimant_cov_det_ak_id--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,eff_from_date,eff_to_date ORDER BY claim_party_occurrence_ak_id DESC) = 1
	),
	LKP_Claim_Paymeny_Category AS (
		SELECT
		cost_containment_saving_amt,
		claim_pay_ak_id
		FROM (
			SELECT claim_payment_category.cost_containment_saving_amt as cost_containment_saving_amt, claim_payment_category.claim_pay_ak_id as claim_pay_ak_id 
			FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_payment_category
			WHERE claim_pay_ctgry_type = 'CC'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_pay_ak_id ORDER BY cost_containment_saving_amt DESC) = 1
	),
	LKP_Claim_Party_Occurrence AS (
		SELECT
		claim_party_occurrence_ak_id,
		claim_occurrence_ak_id,
		claim_party_ak_id,
		claim_case_ak_id,
		claim_occurrence_key,
		source_claim_occurrence_status_code,
		policy_key,
		policy_ak_id,
		Exceed_Claim_Number,
		claimant_num,
		claim_loss_date,
		claim_occurrence_num
		FROM (
			SELECT CPO.claim_occurrence_ak_id             AS claim_occurrence_ak_id,
			       CP.claim_party_ak_id                   AS claim_party_ak_id,
			       CPO.claim_case_ak_id 	           AS claim_case_ak_id,
			       CO.claim_occurrence_key                AS claim_occurrence_key,
			       CO.source_claim_occurrence_status_code AS source_claim_occurrence_status_code,
			       CO.pol_key                             AS policy_key,
			       CO.pol_key_ak_id                    AS policy_ak_id, 
			       CO.s3p_claim_num                       AS Exceed_Claim_Number,
			       CPO.claimant_num                       AS claimant_num,
			       CO.claim_loss_date                     AS claim_loss_date,
			       CO.claim_occurrence_num				  AS claim_occurrence_num,
			       CPO.claim_party_occurrence_ak_id       AS claim_party_occurrence_ak_id
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party_occurrence CPO,
			       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence CO,
			       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_party CP
			WHERE  CPO.claim_occurrence_ak_id = CO.claim_occurrence_ak_id
			       AND CP.claim_party_ak_id = CPO.claim_party_ak_id
			       AND CO.crrnt_snpsht_flag = 1
			       AND CPO.crrnt_snpsht_flag = 1
			       AND CP.crrnt_snpsht_flag = 1
			       AND CPO.claim_party_role_code IN ( 'CLMT', 'CMT' )
			 ORDER  BY claimant_num, claim_party_role_code --
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY claim_party_occurrence_ak_id DESC) = 1
	),
	LKP_Workers_Comp_Claimant_Detail AS (
		SELECT
		wc_claimant_det_ak_id,
		claim_party_occurrence_ak_id
		FROM (
			SELECT WCCD.wc_claimant_det_ak_id        AS wc_claimant_det_ak_id,
			       WCCD.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.workers_comp_claimant_detail  WCCD
			WHERE WCCD.crrnt_snpsht_flag =1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id ORDER BY wc_claimant_det_ak_id DESC) = 1
	),
	LKP_Claim_Case AS (
		SELECT
		claim_case_ak_id,
		prim_litigation_handler_ak_id
		FROM (
			SELECT CC.prim_litigation_handler_ak_id     AS prim_litigation_handler_ak_id,
			       CC.suit_status_code                  AS suit_status_code,
			       CC.prim_litigation_handler_role_code AS prim_litigation_handler_role_code,
			       CC.suit_open_date                    AS suit_open_date,
			       CC.suit_close_date                   AS suit_close_date,
			       CC.claim_case_ak_id                  AS claim_case_ak_id
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_case CC
			WHERE CC.crrnt_snpsht_flag =1
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_ak_id ORDER BY claim_case_ak_id DESC) = 1
	),
	Claim_Representative_H AS (
		SELECT
		claim_rep_ak_id,
		claim_occurrence_ak_id
		FROM (
			SELECT CR.claim_rep_ak_id         AS claim_rep_ak_id,
			          CRO.claim_occurrence_ak_id AS claim_occurrence_ak_id
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative CR,
			       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_occurrence CRO
			WHERE  CR.claim_rep_ak_id = CRO.claim_rep_ak_id
			       AND CR.crrnt_snpsht_flag = 1
			       AND CRO.crrnt_snpsht_flag = 1
			       AND CRO.claim_rep_role_code = 'H'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id ORDER BY claim_rep_ak_id) = 1
	),
	Claim_Representative_Examiner AS (
		SELECT
		claim_rep_ak_id,
		claim_occurrence_ak_id
		FROM (
			SELECT CR.claim_rep_ak_id         AS claim_rep_ak_id,
			       CRO.claim_occurrence_ak_id AS claim_occurrence_ak_id
			FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative CR,
			       @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_representative_occurrence CRO
			WHERE  CR.claim_rep_ak_id = CRO.claim_rep_ak_id
			       AND CR.crrnt_snpsht_flag = 1
			       AND CRO.crrnt_snpsht_flag = 1
			       AND CRO.claim_rep_role_code = 'E'
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id ORDER BY claim_rep_ak_id DESC) = 1
	),
	LKP_Policy_Contract_Customer AS (
		SELECT
		pol_ak_id,
		contract_cust_ak_id,
		agency_ak_id,
		pol_key,
		mco,
		pol_eff_date,
		pol_exp_date,
		pms_pol_lob_code,
		ClassOfBusiness,
		variation_code,
		IN_policy_ak_id
		FROM (
			SELECT       P.contract_cust_ak_id AS contract_cust_ak_id,
			       P.AgencyAKID      AS agency_ak_id,
			       P.pol_key             AS pol_key,
			       P.mco                    AS mco,
			       P.pol_eff_date        AS pol_eff_date,
			       P.pol_exp_date        AS pol_exp_date,
			       P.pms_pol_lob_code    AS pms_pol_lob_code,
			       P.ClassOfBusiness as ClassOfBusiness,
			       P.variation_code      AS variation_code,
			       P.pol_ak_id           AS pol_ak_id
			FROM   
			       @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer CC,
			       @{pipeline().parameters.SOURCE_TABLE_OWNER}.policy P, 
			       @{pipeline().parameters.SOURCE_TABLE_OWNER}.agency A
			WHERE  CC.contract_cust_ak_id = P.contract_cust_ak_id
			       AND P.AgencyAKId  = A.AgencyAKId
			       AND CC.crrnt_snpsht_flag = 1
			       AND P.crrnt_snpsht_flag = 1
			       AND A.CurrentSnapshotFlag =1
			       AND P.pol_ak_id  IN (SELECT DISTINCT pol_key_ak_id FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence)
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id ORDER BY pol_ak_id DESC) = 1
	),
	EXP_Values AS (
		SELECT
		LKP_Policy_Contract_Customer.IN_policy_ak_id AS pol_ak_id,
		LKP_Policy_Contract_Customer.contract_cust_ak_id,
		LKP_Policy_Contract_Customer.agency_ak_id,
		LKP_Policy_Contract_Customer.pol_key,
		LKP_Policy_Contract_Customer.mco,
		LKP_Policy_Contract_Customer.pol_eff_date,
		LKP_Policy_Contract_Customer.pol_exp_date,
		LKP_Policy_Contract_Customer.pms_pol_lob_code,
		LKP_Policy_Contract_Customer.ClassOfBusiness,
		LKP_Policy_Contract_Customer.variation_code,
		LKP_Claim_Party_Occurrence.claim_party_occurrence_ak_id,
		LKP_Claim_Party_Occurrence.claim_occurrence_ak_id,
		LKP_Claim_Party_Occurrence.claim_party_ak_id,
		Claim_Representative_H.claim_rep_ak_id AS claim_rep_ak_id_H,
		Claim_Representative_Examiner.claim_rep_ak_id AS claim_rep_ak_id_E,
		LKP_Claim_Case.claim_case_ak_id,
		LKP_Workers_Comp_Claimant_Detail.wc_claimant_det_ak_id,
		LKP_Claim_Case.prim_litigation_handler_ak_id,
		LKP_Claim_Party_Occurrence.claim_occurrence_key,
		LKP_Claim_Party_Occurrence.source_claim_occurrence_status_code,
		LKP_Claim_Party_Occurrence.policy_key,
		LKP_Claim_Party_Occurrence.Exceed_Claim_Number,
		LKP_Claim_Party_Occurrence.claimant_num,
		LKP_Claim_Party_Occurrence.claim_loss_date,
		LKP_Claim_Party_Occurrence.claim_occurrence_num,
		LKP_Claimant_Coverage_Detail.loc_unit_num,
		LKP_Claimant_Coverage_Detail.sub_loc_unit_num,
		LKP_Claimant_Coverage_Detail.ins_line,
		LKP_Claimant_Coverage_Detail.risk_unit_grp,
		LKP_Claimant_Coverage_Detail.risk_unit_grp_seq_num,
		LKP_Claimant_Coverage_Detail.risk_unit,
		LKP_Claimant_Coverage_Detail.risk_unit_seq_num,
		LKP_Claimant_Coverage_Detail.risk_type_ind,
		LKP_Claimant_Coverage_Detail.major_peril_code,
		LKP_Claimant_Coverage_Detail.major_peril_seq,
		LKP_Claimant_Coverage_Detail.pms_loss_disability,
		LKP_Claimant_Coverage_Detail.reserve_ctgry,
		LKP_Claimant_Coverage_Detail.cause_of_loss,
		LKP_Claimant_Coverage_Detail.pms_type_exposure,
		LKP_Claimant_Coverage_Detail.pms_type_bureau_code,
		LKP_Claim_Paymeny_Category.cost_containment_saving_amt,
		LKP_Claimant_Coverage_Detail.PolicySourceID,
		LKP_Claimant_Coverage_Detail.ClassCode,
		LKP_Claimant_Coverage_Detail.SublineCode,
		LKP_Claimant_Coverage_Detail.RatingCoverageAKId,
		LKP_Claimant_Coverage_Detail.StatisticalCoverageAKID,
		LKP_Claimant_Coverage_Detail.PolicyCoverageAKID,
		LKP_Claimant_Coverage_Detail.RiskLocationAKID,
		LKP_Claimant_Coverage_Detail.PremiumTransactionAKID,
		LKP_Claimant_Coverage_Detail.BureauStatisticalCodeAKID,
		LKP_Claimant_Coverage_Detail.RiskTerritory,
		LKP_Claimant_Coverage_Detail.StateProvinceCode,
		LKP_Claimant_Coverage_Detail.ZipPostalCode,
		LKP_Claimant_Coverage_Detail.TaxLocation,
		LKP_Claimant_Coverage_Detail.Exposure,
		LKP_Claimant_Coverage_Detail.BureauStatisticalcode1_15,
		LKP_Claimant_Coverage_Detail.BureauSpecialUseCode,
		LKP_Claimant_Coverage_Detail.PMSAnnualStatementLine,
		LKP_Claimant_Coverage_Detail.RatingDateIndicator,
		LKP_Claimant_Coverage_Detail.BureauStatisticalUserLine,
		LKP_Claimant_Coverage_Detail.AuditReinstatementIndicator
		FROM 
		LEFT JOIN Claim_Representative_Examiner
		ON Claim_Representative_Examiner.claim_occurrence_ak_id = LKP_Claim_Party_Occurrence.claim_occurrence_ak_id
		LEFT JOIN Claim_Representative_H
		ON Claim_Representative_H.claim_occurrence_ak_id = LKP_Claim_Party_Occurrence.claim_occurrence_ak_id
		LEFT JOIN LKP_Claim_Case
		ON LKP_Claim_Case.claim_case_ak_id = LKP_Claim_Party_Occurrence.claim_case_ak_id
		LEFT JOIN LKP_Claim_Party_Occurrence
		ON LKP_Claim_Party_Occurrence.claim_party_occurrence_ak_id = LKP_Claimant_Coverage_Detail.claim_party_occurrence_ak_id
		LEFT JOIN LKP_Claim_Paymeny_Category
		ON LKP_Claim_Paymeny_Category.claim_pay_ak_id = INPUT.IN_claim_pay_ak_id
		LEFT JOIN LKP_Claimant_Coverage_Detail
		ON LKP_Claimant_Coverage_Detail.claimant_cov_det_ak_id = INPUT.IN_claimant_cov_det_ak_id AND LKP_Claimant_Coverage_Detail.eff_from_date <= INPUT.IN_Date AND LKP_Claimant_Coverage_Detail.eff_to_date >= INPUT.IN_Date
		LEFT JOIN LKP_Policy_Contract_Customer
		ON LKP_Policy_Contract_Customer.pol_ak_id = LKP_Claim_Party_Occurrence.policy_ak_id
		LEFT JOIN LKP_Workers_Comp_Claimant_Detail
		ON LKP_Workers_Comp_Claimant_Detail.claim_party_occurrence_ak_id = LKP_Claimant_Coverage_Detail.claim_party_occurrence_ak_id
	),
	OUTPUT AS (
		SELECT
		pol_ak_id, 
		contract_cust_ak_id, 
		agency_ak_id, 
		pol_key, 
		mco, 
		pol_eff_date, 
		pol_exp_date, 
		pms_pol_lob_code, 
		ClassOfBusiness, 
		variation_code, 
		claim_party_occurrence_ak_id, 
		claim_occurrence_ak_id, 
		claim_party_ak_id, 
		claim_rep_ak_id_H, 
		claim_rep_ak_id_E, 
		claim_case_ak_id, 
		wc_claimant_det_ak_id, 
		claim_occurrence_key, 
		source_claim_occurrence_status_code, 
		policy_key, 
		Exceed_Claim_Number, 
		claimant_num, 
		claim_loss_date, 
		claim_occurrence_num, 
		loc_unit_num, 
		sub_loc_unit_num, 
		ins_line, 
		risk_unit_grp, 
		risk_unit_grp_seq_num, 
		risk_unit, 
		risk_unit_seq_num, 
		risk_type_ind, 
		major_peril_code, 
		major_peril_seq, 
		pms_loss_disability, 
		reserve_ctgry, 
		cause_of_loss, 
		pms_type_exposure, 
		pms_type_bureau_code, 
		cost_containment_saving_amt, 
		PolicySourceID, 
		ClassCode, 
		SublineCode, 
		RatingCoverageAKId, 
		StatisticalCoverageAKID, 
		PolicyCoverageAKID, 
		RiskLocationAKID, 
		PremiumTransactionAKID, 
		BureauStatisticalCodeAKID, 
		RiskTerritory, 
		StateProvinceCode, 
		ZipPostalCode, 
		TaxLocation, 
		Exposure, 
		BureauStatisticalcode1_15, 
		BureauSpecialUseCode, 
		PMSAnnualStatementLine, 
		RatingDateIndicator, 
		BureauStatisticalUserLine, 
		AuditReinstatementIndicator
		FROM EXP_Values
	),
),
EXP_Determine_Loss_Master_Output_Rows AS (
	SELECT
	EXP_Loss_Master_Offset_Onset.claim_trans_id,
	EXP_Loss_Master_Offset_Onset.claim_trans_ak_id,
	mplt_LM_Policy_n_Claim_Attributes.claim_party_occurrence_ak_id,
	mplt_LM_Policy_n_Claim_Attributes.claim_party_ak_id,
	mplt_LM_Policy_n_Claim_Attributes.claim_occurrence_ak_id,
	EXP_Loss_Master_Offset_Onset.claimant_cov_det_ak_id,
	EXP_Loss_Master_Offset_Onset.claim_pay_ak_id,
	mplt_LM_Policy_n_Claim_Attributes.pol_ak_id,
	mplt_LM_Policy_n_Claim_Attributes.contract_cust_ak_id,
	mplt_LM_Policy_n_Claim_Attributes.agency_ak_id,
	mplt_LM_Policy_n_Claim_Attributes.pol_eff_date,
	mplt_LM_Policy_n_Claim_Attributes.pol_exp_date,
	mplt_LM_Policy_n_Claim_Attributes.pms_pol_lob_code,
	mplt_LM_Policy_n_Claim_Attributes.variation_code,
	-- *INF*: IIF(IN(pms_pol_lob_code,'ACA','AFA','APA','ATA','ACJ','AFJ','APJ'),'6',variation_code)
	IFF(pms_pol_lob_code IN ('ACA','AFA','APA','ATA','ACJ','AFJ','APJ'), '6', variation_code) AS LM_Variation_Code,
	mplt_LM_Policy_n_Claim_Attributes.claim_rep_ak_id_H,
	mplt_LM_Policy_n_Claim_Attributes.claim_rep_ak_id_E,
	mplt_LM_Policy_n_Claim_Attributes.claim_case_ak_id,
	mplt_LM_Policy_n_Claim_Attributes.wc_claimant_det_ak_id,
	mplt_LM_Policy_n_Claim_Attributes.claim_occurrence_key,
	mplt_LM_Policy_n_Claim_Attributes.source_claim_occurrence_status_code,
	mplt_LM_Policy_n_Claim_Attributes.policy_key AS Policy_key,
	mplt_LM_Policy_n_Claim_Attributes.Exceed_Claim_Number AS s3p_Claim_Num,
	mplt_LM_Policy_n_Claim_Attributes.claimant_num,
	mplt_LM_Policy_n_Claim_Attributes.claim_loss_date AS Claim_loss_date,
	-- *INF*: SUBSTR(Policy_key,1,3)
	SUBSTR(Policy_key, 1, 3) AS V_Policy_Symbol,
	V_Policy_Symbol AS Policy_Symbol,
	-- *INF*: SUBSTR(Policy_key,4,7)
	SUBSTR(Policy_key, 4, 7) AS V_Policy_Number,
	V_Policy_Number AS Policy_Number,
	-- *INF*: SUBSTR(Policy_key,11,2)
	SUBSTR(Policy_key, 11, 2) AS V_Policy_Module,
	V_Policy_Module AS Policy_Module,
	mplt_LM_Policy_n_Claim_Attributes.loc_unit_num,
	-- *INF*: IIF(loc_unit_num = 'N/A','0000',loc_unit_num)
	-- 
	-- 
	-- --Adding new rules for Personal Lines policy as the coverage EDW is incorrect.
	-- ----IIF(loc_unit_num = 'N/A','0000',loc_unit_num)
	-- 
	-- -----IIF(loc_unit_num = 'N/A','0000',
	--     --   IIF(SUBSTR(Policy_key,1,1)='H' and SUBSTR(Policy_key,4,1) = '5','0000',loc_unit_num)
	--        ---)
	IFF(loc_unit_num = 'N/A', '0000', loc_unit_num) AS v_loc_unit_num,
	v_loc_unit_num AS loc_unit_num_out,
	mplt_LM_Policy_n_Claim_Attributes.sub_loc_unit_num,
	-- *INF*: IIF(sub_loc_unit_num='N/A','000',sub_loc_unit_num)
	IFF(sub_loc_unit_num = 'N/A', '000', sub_loc_unit_num) AS v_sub_loc_unit_num,
	v_sub_loc_unit_num AS sub_loc_unit_num_out,
	mplt_LM_Policy_n_Claim_Attributes.ins_line,
	-- *INF*: IIF(ins_line = 'N/A','NA',ins_line)
	IFF(ins_line = 'N/A', 'NA', ins_line) AS ins_line_out,
	mplt_LM_Policy_n_Claim_Attributes.risk_unit_grp,
	-- *INF*: IIF(risk_unit_grp = 'N/A','000',risk_unit_grp)
	IFF(risk_unit_grp = 'N/A', '000', risk_unit_grp) AS risk_unit_grp_out,
	mplt_LM_Policy_n_Claim_Attributes.risk_unit_grp_seq_num,
	-- *INF*: IIF(LENGTH(RTRIM(risk_unit_grp_seq_num))<3,LPAD(RTRIM(risk_unit_grp_seq_num),3,'0'),risk_unit_grp_seq_num)
	IFF(
	    LENGTH(RTRIM(risk_unit_grp_seq_num)) < 3, LPAD(RTRIM(risk_unit_grp_seq_num), 3, '0'),
	    risk_unit_grp_seq_num
	) AS v_risk_unit_grp_seq_num,
	-- *INF*: IIF(SUBSTR(v_risk_unit_grp_seq_num,1,2)='N/','NA',SUBSTR(v_risk_unit_grp_seq_num,1,2))
	IFF(
	    SUBSTR(v_risk_unit_grp_seq_num, 1, 2) = 'N/', 'NA', SUBSTR(v_risk_unit_grp_seq_num, 1, 2)
	) AS risk_unit_grp_seq_num_First_2pos,
	-- *INF*: IIF(SUBSTR(v_risk_unit_grp_seq_num,3,1)='A','N',SUBSTR(v_risk_unit_grp_seq_num,3,1))
	IFF(SUBSTR(v_risk_unit_grp_seq_num, 3, 1) = 'A', 'N', SUBSTR(v_risk_unit_grp_seq_num, 3, 1)) AS risk_unit_grp_seq_num_last_pos,
	mplt_LM_Policy_n_Claim_Attributes.risk_unit,
	-- *INF*: RTRIM(risk_unit)
	RTRIM(risk_unit) AS risk_unit_out,
	-- *INF*: SUBSTR(risk_unit,1,3)
	-- 
	-- 
	-- ---IIF(source_sys_id = 'PMS',SUBSTR(risk_unit,1,3),
	-- ---IIF(SUBSTR(Policy_key,1,1)='H' and SUBSTR(Policy_key,4,1) = '5',loc_unit_num,SUBSTR(risk_unit,1,3)))
	-- 
	-- ---SUBSTR(risk_unit,1,3)
	SUBSTR(risk_unit, 1, 3) AS risk_unit_First_3pos,
	-- *INF*: IIF(LENGTH(RTRIM(LTRIM(SUBSTR(risk_unit,4,3))))<3,
	-- RPAD(RTRIM(LTRIM(SUBSTR(risk_unit,4,3))),3,'0'), RTRIM(LTRIM(SUBSTR(risk_unit,4,3)))
	-- )
	IFF(
	    LENGTH(RTRIM(LTRIM(SUBSTR(risk_unit, 4, 3)))) < 3,
	    RPAD(RTRIM(LTRIM(SUBSTR(risk_unit, 4, 3))), 3, '0'),
	    RTRIM(LTRIM(SUBSTR(risk_unit, 4, 3)))
	) AS risk_unit_last_3pos,
	mplt_LM_Policy_n_Claim_Attributes.risk_unit_seq_num,
	mplt_LM_Policy_n_Claim_Attributes.risk_type_ind,
	-- *INF*: IIF(risk_unit_seq_num ='0' and risk_type_ind = 'N/A','00',
	-- IIF(LENGTH(risk_unit_seq_num)=1 and risk_unit_seq_num <> '0' and risk_type_ind = 'N/A', risk_unit_seq_num || '0',risk_unit_seq_num || risk_type_ind ))
	IFF(
	    risk_unit_seq_num = '0' and risk_type_ind = 'N/A', '00',
	    IFF(
	        LENGTH(risk_unit_seq_num) = 1
	    and risk_unit_seq_num <> '0'
	    and risk_type_ind = 'N/A',
	        risk_unit_seq_num || '0',
	        risk_unit_seq_num || risk_type_ind
	    )
	) AS risk_unit_seq_num_out,
	mplt_LM_Policy_n_Claim_Attributes.major_peril_code,
	-- *INF*: IIF(major_peril_code='N/A','000',major_peril_code)
	IFF(major_peril_code = 'N/A', '000', major_peril_code) AS major_peril_code_Out,
	mplt_LM_Policy_n_Claim_Attributes.major_peril_seq,
	-- *INF*: IIF(major_peril_seq='N/A','00',major_peril_seq)
	IFF(major_peril_seq = 'N/A', '00', major_peril_seq) AS major_peril_seq_out,
	mplt_LM_Policy_n_Claim_Attributes.pms_loss_disability,
	mplt_LM_Policy_n_Claim_Attributes.reserve_ctgry,
	mplt_LM_Policy_n_Claim_Attributes.cause_of_loss,
	mplt_LM_Policy_n_Claim_Attributes.pms_type_exposure,
	-- *INF*: IIF(pms_type_exposure = 'N/A','000',pms_type_exposure)
	IFF(pms_type_exposure = 'N/A', '000', pms_type_exposure) AS pms_type_exposure_out,
	mplt_LM_Policy_n_Claim_Attributes.pms_type_bureau_code,
	mplt_LM_Policy_n_Claim_Attributes.cost_containment_saving_amt,
	EXP_Loss_Master_Offset_Onset.sar_id,
	EXP_Loss_Master_Offset_Onset.financial_type_code,
	EXP_Loss_Master_Offset_Onset.s3p_trans_code,
	EXP_Loss_Master_Offset_Onset.pms_trans_code,
	EXP_Loss_Master_Offset_Onset.trans_code,
	EXP_Loss_Master_Offset_Onset.trans_date,
	EXP_Loss_Master_Offset_Onset.pms_acct_entered_date,
	EXP_Loss_Master_Offset_Onset.trans_base_type_code,
	EXP_Loss_Master_Offset_Onset.trans_ctgry_code,
	EXP_Loss_Master_Offset_Onset.Exceed_Trans_Code,
	-- *INF*: TO_CHAR(trans_date,'YYYYMMDD')
	TO_CHAR(trans_date, 'YYYYMMDD') AS V_Trans_Date_char,
	-- *INF*: TO_CHAR(pms_acct_entered_date,'YYYYMMDD')
	-- 
	-- 
	TO_CHAR(pms_acct_entered_date, 'YYYYMMDD') AS V_pms_acct_entered_date_char,
	-- *INF*: SUBSTR(V_Trans_Date_char,1,6)
	-- 
	-- ----IIF(SUBSTR(V_Trans_Date_char,1,6) <= SUBSTR(V_pms_acct_entered_date_char,1,6), 
	-- ----                                SUBSTR(V_pms_acct_entered_date_char,1,6), SUBSTR(V_Trans_Date_char,1,6))
	-- 
	-- 
	-- 
	SUBSTR(V_Trans_Date_char, 1, 6) AS V_Trans_PMS_Account_Date,
	V_Trans_PMS_Account_Date AS Transaction_Account_Date,
	EXP_Loss_Master_Offset_Onset.trans_amt,
	EXP_Loss_Master_Offset_Onset.trans_hist_amt AS IN_trans_hist_amt,
	-- *INF*: IIF(IN (pms_trans_code,'97','98','99','26','27','81','83','82','84','89','75','76') 
	-- AND source_sys_id <> 'PMS', -1 * IN_trans_hist_amt, IN_trans_hist_amt)
	-- 
	-- --- Added 76 trans_code
	-- 
	IFF(
	    pms_trans_code IN ('97','98','99','26','27','81','83','82','84','89','75','76')
	    and source_sys_id <> 'PMS',
	    - 1 * IN_trans_hist_amt,
	    IN_trans_hist_amt
	) AS v_trans_hist_amt,
	v_trans_hist_amt AS trans_hist_amt,
	EXP_Loss_Master_Offset_Onset.source_sys_id,
	-- *INF*: ADD_TO_DATE(SYSDATE,'MM',@{pipeline().parameters.NO_OF_MONTHS})
	DATEADD(MONTH,@{pipeline().parameters.NO_OF_MONTHS},CURRENT_TIMESTAMP) AS V_Last_Month_Date,
	-- *INF*: LAST_DAY(V_Last_Month_Date)
	LAST_DAY(V_Last_Month_Date) AS V_Last_Month_Last_Day_Date,
	-- *INF*: GET_DATE_PART(V_Last_Month_Last_Day_Date,'YYYY')
	DATE_PART(V_Last_Month_Last_Day_Date, 'YYYY') AS V_Account_Date_YYYY,
	-- *INF*: GET_DATE_PART(V_Last_Month_Last_Day_Date,'MM')
	DATE_PART(V_Last_Month_Last_Day_Date, 'MM') AS V_Account_Date_MM,
	-- *INF*: V_Account_Date_YYYY || LPAD(V_Account_Date_MM,2,'0')
	V_Account_Date_YYYY || LPAD(V_Account_Date_MM, 2, '0') AS V_Account_Date,
	-- *INF*: V_Account_Date
	-- ---@{pipeline().parameters.ACCOUNT_DATE}
	-- ---- Date of the previous month, as we are processing previous months data.
	V_Account_Date AS V_Account_Date_To_Check,
	V_Account_Date AS PMS_Account_Date_To_Check,
	-- *INF*: IIF
	-- (
	--     (NOT IN(pms_trans_code,'43','65','66','91') 
	--            AND (V_Trans_PMS_Account_Date = V_Account_Date_To_Check OR SUBSTR(V_pms_acct_entered_date_char,1,6) = V_Account_Date_To_Check )
	--      )
	--    OR 
	--     (IN(pms_trans_code,'90','92','95','97','98','99')  
	--     --AND SUBSTR(source_claim_occurrence_status_code,1,1) = 'O' 
	--     --AND SUBSTR(source_claim_occurrence_status_code,1,3) <> 'OFF'
	--     ),
	-- 'VALID','INVALID'
	-- )
	-- 
	-- ----3/28/2011  OR V_pms_acct_entered_date_char = V_Account_Date_To_Check  : To include the Offset records of exceed claims into calc table.
	-- 
	-- ---- 3/14/2011  Added the condition to check on pms_acct_entered_date for EXCEED Offset_Onset Data
	IFF(
	    (NOT pms_trans_code IN ('43','65','66','91')
	    and (V_Trans_PMS_Account_Date = V_Account_Date_To_Check
	    or SUBSTR(V_pms_acct_entered_date_char, 1, 6) = V_Account_Date_To_Check))
	    or (pms_trans_code IN ('90','92','95','97','98','99')),
	    'VALID',
	    'INVALID'
	) AS V_Valid_Claim_Transaction,
	-- *INF*: IIF(NOT ISNULL(claim_trans_id), 
	--            IIF(V_Valid_Claim_Transaction = 'VALID' AND NOT IN(Exceed_Trans_Code,'E66','E65','B65','B66','R65','R66','S65','S66'),'VALID','INVALID'),
	--              IIF(V_Valid_Claim_Transaction = 'VALID' ,'VALID','INVALID')
	-- )
	-- 
	-- 
	-- ---4/11/2011- Below logic  was changed to the above so that different logic for Claim_Transaction_Records and Claim_Reinsurance_transaction records as there is not s3p_trans_code for Reinsurance_Transactions.
	-- 
	-- ---IIF(V_Valid_Claim_Transaction = 'VALID' AND NOT IN(Exceed_Trans_Code,'E66','E65','B65','B66','R65','R66','S65','S66'),'VALID','INVALID')
	-- 
	-- 
	-- 
	-- -------------------------------------------------
	-- --DECODE(TRUE, 
	-- ---IIF(NOT ISNULL(claim_trans_id),
	-- ---V_Valid_Claim_Transaction = 'VALID' AND NOT IN(Exceed_Trans_Code,'E66','E65','B65','B66','R65','R66','S65','S66'),'VALID',
	-- ---V_Valid_Claim_Transaction = 'VALID','VALID',
	-- --'INVALID')
	-- --)
	-- 
	IFF(
	    claim_trans_id IS NOT NULL,
	    IFF(
	        V_Valid_Claim_Transaction = 'VALID'
	        and NOT Exceed_Trans_Code IN ('E66','E65','B65','B66','R65','R66','S65','S66'),
	        'VALID',
	        'INVALID'
	    ),
	    IFF(
	        V_Valid_Claim_Transaction = 'VALID', 'VALID', 'INVALID'
	    )
	) AS Valid_Claim_Transaction,
	-- *INF*: IIF(IN(pms_trans_code,'90','92','95','97','98','99') AND pms_trans_code <> '95',trans_amt,0.0)
	IFF(
	    pms_trans_code IN ('90','92','95','97','98','99') AND pms_trans_code <> '95', trans_amt, 0.0
	) AS V_LM_Amount_OutStanding,
	-- *INF*: IIF(IN(pms_trans_code,'97','98','99') AND source_sys_id <> 'PMS', -1 * V_LM_Amount_OutStanding,V_LM_Amount_OutStanding)
	-- 
	-- 
	-- --- When Exceed data backfeeds to PMS, B90,B91 become as 99 and sign on the amount field changes to -ve sign aling with value. 
	-- --- Changed the sign of the amount field only for EXCEED EDW Data
	-- ---- 11/12/2010 Added other transaction code 97, 98
	IFF(
	    pms_trans_code IN ('97','98','99') AND source_sys_id <> 'PMS', - 1 * V_LM_Amount_OutStanding,
	    V_LM_Amount_OutStanding
	) AS LM_Amount_OutStanding,
	-- *INF*: IIF(NOT IN(pms_trans_code,'90','92','95','97','98','99') AND NOT IN(pms_trans_code,'71','72','73','74','75','76','77','78',   '79'),trans_amt,0.0)
	IFF(
	    NOT pms_trans_code IN ('90','92','95','97','98','99')
	    and NOT pms_trans_code IN ('71','72','73','74','75','76','77','78','79'),
	    trans_amt,
	    0.0
	) AS V_LM_Amount_Paid_Losses,
	-- *INF*: IIF(IN(pms_trans_code,'26','27','37','81','83','82','84','88','89') AND source_sys_id <>'PMS', 
	-- -1 * V_LM_Amount_Paid_Losses, V_LM_Amount_Paid_Losses)
	-- 
	-- 
	-- 
	-- ---- B22,B23,B24 convert to 82,83,84 into PMS during backfeed and amount field is multiplied by a -ve sign.
	-- ---- 26,27 trans_code in PMS relate to Recovery so the amount field is multiplied by a -ve sign
	IFF(
	    pms_trans_code IN ('26','27','37','81','83','82','84','88','89') AND source_sys_id <> 'PMS',
	    - 1 * V_LM_Amount_Paid_Losses,
	    V_LM_Amount_Paid_Losses
	) AS LM_Amount_Paid_Losses,
	-- *INF*: IIF(NOT IN(pms_trans_code,'90','92','95','97','98','99') AND IN(pms_trans_code,'71','72','73','74','75','76','77','78','79'),trans_amt,0.0)
	IFF(
	    NOT pms_trans_code IN ('90','92','95','97','98','99')
	    and pms_trans_code IN ('71','72','73','74','75','76','77','78','79'),
	    trans_amt,
	    0.0
	) AS V_LM_Amount_Paid_Expenses,
	-- *INF*: IIF(IN(pms_trans_code ,'75','76') AND source_sys_id <>  'PMS',
	-- -1 * V_LM_Amount_Paid_Expenses, V_LM_Amount_Paid_Expenses)
	-- 
	-- ---- R21EX,R22EX,R23EX,R24EX,R29EX converts into 75 when it backfeeds along with amount field being multiplied with -ve sign.
	-- ---12/10/2010  Added 76 to above as R29EX converts to 76 
	IFF(
	    pms_trans_code IN ('75','76') AND source_sys_id <> 'PMS', - 1 * V_LM_Amount_Paid_Expenses,
	    V_LM_Amount_Paid_Expenses
	) AS LM_Amount_Paid_Expenses,
	-- *INF*: IIF(IN(pms_trans_code,'90','92','95','97','98','99') AND pms_trans_code = '95',trans_amt,0.0)
	IFF(
	    pms_trans_code IN ('90','92','95','97','98','99') AND pms_trans_code = '95', trans_amt, 0.0
	) AS V_LM_Unpaid_Loss_Adj_Exp,
	V_LM_Unpaid_Loss_Adj_Exp AS LM_Unpaid_Loss_Adj_Exp,
	-- *INF*: SET_DATE_PART(
	--          SET_DATE_PART(
	--                      SET_DATE_PART( V_Last_Month_Last_Day_Date, 'HH', 23) 
	--                                           ,'MI',59)
	--                                ,'SS',59)
	-- 
	-- 
	-- --- Changing the date to have 00:00:00 in the timestamp part
	-- 
	DATEADD(SECOND,59-DATE_PART(SECOND,DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date)),DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date))),DATEADD(MINUTE,59-DATE_PART(MINUTE,DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date)),DATEADD(HOUR,23-DATE_PART(HOUR,V_Last_Month_Last_Day_Date),V_Last_Month_Last_Day_Date))) AS Loss_Master_Run_Date,
	EXP_Loss_Master_Offset_Onset.trans_offset_onset_ind,
	-- *INF*: IIF(IN(pms_trans_code,'90','92','97','98','99')=1,IN_LM_override_amt ,0.0)
	IFF(pms_trans_code IN ('90','92','97','98','99') = 1, IN_LM_override_amt, 0.0) AS LM_Amount_Outstanding_Override,
	-- *INF*: IIF(IN(pms_trans_code,'90','92','95','97','98','99','71','72','73','74','75','76','77','78', '79')=0,IN_LM_override_amt,0.0)
	IFF(
	    pms_trans_code IN ('90','92','95','97','98','99','71','72','73','74','75','76','77','78','79') = 0,
	    IN_LM_override_amt,
	    0.0
	) AS LM_Amount_Paid_Losses_Override,
	-- *INF*: IIF( IN(pms_trans_code,'71','72','73','74','75','76','77','78','79')=1,IN_LM_override_amt,0.0)
	IFF(
	    pms_trans_code IN ('71','72','73','74','75','76','77','78','79') = 1, IN_LM_override_amt,
	    0.0
	) AS LM_Amount_Paid_Expenses_Override,
	-- *INF*: IIF(pms_trans_code = '95',IN_LM_override_amt,0.0)
	IFF(pms_trans_code = '95', IN_LM_override_amt, 0.0) AS LM_Unpaid_Loss_Adj_Exp_Override
	FROM EXP_Loss_Master_Offset_Onset
	 -- Manually join with mplt_LM_Policy_n_Claim_Attributes
),
FIL_Claim_Transaction_Rows AS (
	SELECT
	EXP_Determine_Loss_Master_Output_Rows.claim_trans_id, 
	EXP_Determine_Loss_Master_Output_Rows.claim_trans_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows.claim_party_occurrence_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows.claim_occurrence_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows.claimant_cov_det_ak_id AS Claimant_cov_det_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows.claim_pay_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows.claim_party_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows.pol_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows.contract_cust_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows.agency_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows.pol_eff_date, 
	EXP_Determine_Loss_Master_Output_Rows.pol_exp_date, 
	EXP_Determine_Loss_Master_Output_Rows.claim_rep_ak_id_H, 
	EXP_Determine_Loss_Master_Output_Rows.claim_rep_ak_id_E, 
	EXP_Determine_Loss_Master_Output_Rows.claim_case_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows.wc_claimant_det_ak_id, 
	EXP_Determine_Loss_Master_Output_Rows.claim_occurrence_key, 
	EXP_Determine_Loss_Master_Output_Rows.source_claim_occurrence_status_code, 
	EXP_Determine_Loss_Master_Output_Rows.Policy_key, 
	EXP_Determine_Loss_Master_Output_Rows.s3p_Claim_Num, 
	EXP_Determine_Loss_Master_Output_Rows.Claim_loss_date, 
	EXP_Determine_Loss_Master_Output_Rows.LM_Variation_Code, 
	EXP_Determine_Loss_Master_Output_Rows.Policy_Symbol, 
	EXP_Determine_Loss_Master_Output_Rows.Policy_Number, 
	EXP_Determine_Loss_Master_Output_Rows.Policy_Module, 
	EXP_Determine_Loss_Master_Output_Rows.loc_unit_num, 
	EXP_Determine_Loss_Master_Output_Rows.loc_unit_num_out, 
	EXP_Determine_Loss_Master_Output_Rows.sub_loc_unit_num, 
	EXP_Determine_Loss_Master_Output_Rows.sub_loc_unit_num_out, 
	EXP_Determine_Loss_Master_Output_Rows.ins_line, 
	EXP_Determine_Loss_Master_Output_Rows.ins_line_out, 
	EXP_Determine_Loss_Master_Output_Rows.risk_unit_grp, 
	EXP_Determine_Loss_Master_Output_Rows.risk_unit_grp_out, 
	EXP_Determine_Loss_Master_Output_Rows.risk_unit_grp_seq_num, 
	EXP_Determine_Loss_Master_Output_Rows.risk_unit_grp_seq_num_First_2pos, 
	EXP_Determine_Loss_Master_Output_Rows.risk_unit_grp_seq_num_last_pos, 
	EXP_Determine_Loss_Master_Output_Rows.risk_unit_out AS risk_unit, 
	EXP_Determine_Loss_Master_Output_Rows.risk_unit_seq_num, 
	EXP_Determine_Loss_Master_Output_Rows.risk_unit_seq_num_out, 
	EXP_Determine_Loss_Master_Output_Rows.major_peril_code, 
	EXP_Determine_Loss_Master_Output_Rows.major_peril_code_Out, 
	EXP_Determine_Loss_Master_Output_Rows.major_peril_seq, 
	EXP_Determine_Loss_Master_Output_Rows.major_peril_seq_out, 
	EXP_Determine_Loss_Master_Output_Rows.pms_loss_disability, 
	EXP_Determine_Loss_Master_Output_Rows.reserve_ctgry, 
	EXP_Determine_Loss_Master_Output_Rows.cause_of_loss, 
	EXP_Determine_Loss_Master_Output_Rows.pms_type_exposure, 
	EXP_Determine_Loss_Master_Output_Rows.pms_type_exposure_out, 
	EXP_Determine_Loss_Master_Output_Rows.pms_type_bureau_code, 
	EXP_Determine_Loss_Master_Output_Rows.sar_id, 
	EXP_Determine_Loss_Master_Output_Rows.financial_type_code, 
	EXP_Determine_Loss_Master_Output_Rows.s3p_trans_code, 
	EXP_Determine_Loss_Master_Output_Rows.pms_trans_code, 
	EXP_Determine_Loss_Master_Output_Rows.trans_code, 
	EXP_Determine_Loss_Master_Output_Rows.trans_date, 
	EXP_Determine_Loss_Master_Output_Rows.pms_acct_entered_date, 
	EXP_Determine_Loss_Master_Output_Rows.trans_base_type_code, 
	EXP_Determine_Loss_Master_Output_Rows.trans_ctgry_code, 
	EXP_Determine_Loss_Master_Output_Rows.Transaction_Account_Date, 
	EXP_Determine_Loss_Master_Output_Rows.trans_amt, 
	EXP_Determine_Loss_Master_Output_Rows.trans_hist_amt, 
	EXP_Determine_Loss_Master_Output_Rows.source_sys_id, 
	EXP_Determine_Loss_Master_Output_Rows.PMS_Account_Date_To_Check, 
	EXP_Determine_Loss_Master_Output_Rows.Valid_Claim_Transaction, 
	EXP_Determine_Loss_Master_Output_Rows.LM_Amount_OutStanding, 
	EXP_Determine_Loss_Master_Output_Rows.LM_Amount_Paid_Losses, 
	EXP_Determine_Loss_Master_Output_Rows.LM_Amount_Paid_Expenses, 
	EXP_Determine_Loss_Master_Output_Rows.LM_Unpaid_Loss_Adj_Exp, 
	EXP_Determine_Loss_Master_Output_Rows.Loss_Master_Run_Date, 
	EXP_Determine_Loss_Master_Output_Rows.cost_containment_saving_amt, 
	EXP_Determine_Loss_Master_Output_Rows.trans_offset_onset_ind, 
	mplt_LM_Policy_n_Claim_Attributes.StatisticalCoverageAKID, 
	mplt_LM_Policy_n_Claim_Attributes.RatingCoverageAKId AS RatingCoverageAKID, 
	mplt_LM_Policy_n_Claim_Attributes.PolicySourceID, 
	mplt_LM_Policy_n_Claim_Attributes.ClassCode, 
	mplt_LM_Policy_n_Claim_Attributes.SublineCode, 
	mplt_LM_Policy_n_Claim_Attributes.PolicyCoverageAKID, 
	mplt_LM_Policy_n_Claim_Attributes.RiskLocationAKID, 
	mplt_LM_Policy_n_Claim_Attributes.PremiumTransactionAKID, 
	mplt_LM_Policy_n_Claim_Attributes.BureauStatisticalCodeAKID, 
	mplt_LM_Policy_n_Claim_Attributes.RiskTerritory, 
	mplt_LM_Policy_n_Claim_Attributes.StateProvinceCode, 
	mplt_LM_Policy_n_Claim_Attributes.ZipPostalCode, 
	mplt_LM_Policy_n_Claim_Attributes.TaxLocation, 
	mplt_LM_Policy_n_Claim_Attributes.Exposure, 
	mplt_LM_Policy_n_Claim_Attributes.BureauStatisticalcode1_15, 
	mplt_LM_Policy_n_Claim_Attributes.BureauSpecialUseCode, 
	mplt_LM_Policy_n_Claim_Attributes.PMSAnnualStatementLine, 
	mplt_LM_Policy_n_Claim_Attributes.RatingDateIndicator, 
	mplt_LM_Policy_n_Claim_Attributes.BureauStatisticalUserLine, 
	mplt_LM_Policy_n_Claim_Attributes.AuditReinstatementIndicator
	FROM EXP_Determine_Loss_Master_Output_Rows
	 -- Manually join with mplt_LM_Policy_n_Claim_Attributes
	WHERE IIF(Valid_Claim_Transaction = 'VALID',TRUE,FALSE)
),
mplt_Coverage_Temp_Policy_Transaction_Attributes AS (WITH
	INPUT AS (
		
	),
	EXP_Values AS (
		SELECT
		pol_ak_id,
		loss_id,
		ins_line,
		-- *INF*: RTRIM(ins_line)
		RTRIM(ins_line) AS ins_line_out,
		loc_unit_num,
		-- *INF*: RTRIM(loc_unit_num)
		RTRIM(loc_unit_num) AS loc_unit_num1,
		sub_loc_unit_num,
		-- *INF*: RTRIM(sub_loc_unit_num)
		RTRIM(sub_loc_unit_num) AS sub_loc_unit_num1,
		risk_unit_grp,
		-- *INF*: RTRIM(risk_unit_grp)
		RTRIM(risk_unit_grp) AS risk_unit_grp1,
		risk_unit_grp_seq_num_First_2pos,
		-- *INF*: RTRIM(risk_unit_grp_seq_num_First_2pos)
		RTRIM(risk_unit_grp_seq_num_First_2pos) AS risk_unit_grp_seq_num_First_2pos1,
		risk_unit_grp_seq_num_last_pos,
		-- *INF*: RTRIM(risk_unit_grp_seq_num_last_pos)
		RTRIM(risk_unit_grp_seq_num_last_pos) AS risk_unit_grp_seq_num_last_pos1,
		risk_unit_complete,
		-- *INF*: RTRIM(risk_unit_complete)
		RTRIM(risk_unit_complete) AS risk_unit_complete1,
		risk_unit_seq_num,
		-- *INF*: RTRIM(risk_unit_seq_num)
		RTRIM(risk_unit_seq_num) AS risk_unit_seq_num1,
		pms_type_exposure,
		-- *INF*: RTRIM(pms_type_exposure)
		RTRIM(pms_type_exposure) AS pms_type_exposure1,
		major_peril_code,
		-- *INF*: RTRIM(major_peril_code)
		RTRIM(major_peril_code) AS major_peril_code1,
		major_peril_seq,
		-- *INF*: RTRIM(major_peril_seq)
		RTRIM(major_peril_seq) AS major_peril_seq1,
		Claim_loss_date
		FROM INPUT
	),
	LKP_Coverage_Temp_Policy_Transaction AS (
		SELECT
		cov_ak_id,
		temp_pol_trans_ak_id,
		pol_ak_id,
		sar_id,
		ins_line,
		loc_unit_num,
		sub_loc_unit_num,
		risk_unit_grp,
		risk_unit_grp_seq_num_First_2pos,
		risk_unit_grp_seq_num_lastpos,
		risk_unit,
		risk_unit_seq_num,
		major_peril_code,
		major_peril_seq_num,
		pms_type_exposure,
		cov_eff_date,
		type_bureau_code,
		risk_state_prov_code,
		risk_zip_code,
		terr_code,
		tax_loc,
		class_code,
		exposure,
		sub_line_code,
		source_sar_asl,
		source_sar_prdct_line,
		source_sar_sp_use_code,
		source_statistical_code,
		section_code,
		rsn_amended_code,
		part_code,
		rating_date_ind
		FROM (
			SELECT  C.cov_ak_id                     AS cov_ak_id,  
			TPT.temp_pol_trans_ak_id   AS temp_pol_trans_ak_id,  
			CASE  C.pms_type_exposure WHEN 'N/A' THEN '000' ELSE RTRIM(C.pms_type_exposure)   END AS pms_type_exposure,  
			C.type_bureau_code              AS type_bureau_code,  
			TPT.risk_state_prov_code        AS risk_state_prov_code,  
			TPT.risk_zip_code               AS risk_zip_code,  
			TPT.terr_code                   AS terr_code,  
			TPT.tax_loc                     AS tax_loc,  
			TPT.class_code                  AS class_code,  
			TPT.exposure                      AS exposure,  
			TPT.sub_line_code               AS sub_line_code,  
			TPT.source_sar_asl              AS source_sar_asl,  
			TPT.source_sar_prdct_line       AS source_sar_prdct_line,  
			TPT.source_sar_sp_use_code	 AS source_sar_sp_use_code,  
			TPT.source_statistical_code     AS source_statistical_code,  
			TPT.section_code                AS section_code,  
			TPT.rsn_amended_code            AS rsn_amended_code,  
			TPT.part_code                   AS part_code,  
			RTRIM(TPT.rating_date_ind)        AS rating_date_ind,  
			C.pol_ak_id                     AS pol_ak_id,  
			TPT.sar_id                      AS sar_id,  
			CASE C.ins_line WHEN 'N/A' THEN 'NA' ELSE RTRIM(C.ins_line) END AS ins_line,  
			CASE  C.loc_unit_num  WHEN 'N/A' THEN '0000' ELSE RTRIM(C.loc_unit_num)  END AS loc_unit_num,  
			CASE  C.sub_loc_unit_num  WHEN 'N/A' THEN '000' ELSE RTRIM(C.sub_loc_unit_num)   END AS sub_loc_unit_num,  
			CASE  C.risk_unit_grp   WHEN 'N/A' THEN '000' ELSE RTRIM(C.risk_unit_grp)    END AS risk_unit_grp,  
			CASE  SUBSTRING(C.risk_unit_grp_seq_num,1,2)   WHEN 'N/' THEN 'NA' ELSE SUBSTRING(C.risk_unit_grp_seq_num,1,2) END AS risk_unit_grp_seq_num_First_2pos,  
			CASE  SUBSTRING(C.risk_unit_grp_seq_num,3,1)   WHEN 'A' THEN 'N' ELSE SUBSTRING(C.risk_unit_grp_seq_num,3,1) END AS risk_unit_grp_seq_num_lastpos,  
			RTRIM(C.risk_unit)               AS risk_unit,  
			CASE   C.risk_unit_seq_num   WHEN 'N/A' THEN '00' ELSE  RTRIM(C.risk_unit_seq_num)    END AS risk_unit_seq_num,  
			RTRIM(C.major_peril_code)              AS major_peril_code,  
			RTRIM(C.major_peril_seq_num)           AS major_peril_seq_num,  
			C.cov_eff_date                  AS cov_eff_date 
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.coverage C 
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.temp_policy_transaction TPT ON C.cov_ak_id = TPT.cov_ak_id AND C.crrnt_snpsht_flag = 1 AND TPT.crrnt_snpsht_flag = 1
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.Claim_Occurrence CO ON CO.pol_key_ak_id = C.pol_ak_id and CO.crrnt_snpsht_flag = 1
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.Claim_Party_Occurrence CPO ON CPO.Claim_Occurrence_ak_id = CO.Claim_Occurrence_ak_id and CPO.crrnt_snpsht_flag = 1
			INNER JOIN @{pipeline().parameters.TARGET_TABLE_OWNER}.Claimant_Coverage_Detail CCD ON CCD.Claim_Party_Occurrence_ak_id = CPO.Claim_Party_Occurrence_ak_id and CCD.crrnt_snpsht_flag = 1
			AND RTRIM(C.risk_unit) = CCD.Risk_unit AND RTRIM(C.major_peril_code)  = CCD.major_peril_code
			ORDER BY TPT.temp_pol_trans_ak_id   --
			
			--- Order By clause is important in this Lookup Override because how the data is retrieved is important
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id,sar_id,ins_line,loc_unit_num,sub_loc_unit_num,risk_unit_grp,risk_unit_grp_seq_num_First_2pos,risk_unit_grp_seq_num_lastpos,risk_unit,risk_unit_seq_num,major_peril_code,major_peril_seq_num,cov_eff_date ORDER BY cov_ak_id DESC) = 1
	),
	EXP_Lkp_Values AS (
		SELECT
		LKP_Coverage_Temp_Policy_Transaction.temp_pol_trans_ak_id,
		LKP_Coverage_Temp_Policy_Transaction.cov_ak_id,
		LKP_Coverage_Temp_Policy_Transaction.pol_ak_id,
		LKP_Coverage_Temp_Policy_Transaction.sar_id,
		LKP_Coverage_Temp_Policy_Transaction.ins_line,
		LKP_Coverage_Temp_Policy_Transaction.loc_unit_num,
		LKP_Coverage_Temp_Policy_Transaction.sub_loc_unit_num,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_First_2pos,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_lastpos,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit,
		LKP_Coverage_Temp_Policy_Transaction.risk_unit_seq_num,
		LKP_Coverage_Temp_Policy_Transaction.major_peril_code,
		LKP_Coverage_Temp_Policy_Transaction.major_peril_seq_num,
		LKP_Coverage_Temp_Policy_Transaction.pms_type_exposure,
		LKP_Coverage_Temp_Policy_Transaction.cov_eff_date,
		LKP_Coverage_Temp_Policy_Transaction.type_bureau_code,
		LKP_Coverage_Temp_Policy_Transaction.risk_state_prov_code,
		LKP_Coverage_Temp_Policy_Transaction.risk_zip_code,
		LKP_Coverage_Temp_Policy_Transaction.terr_code,
		LKP_Coverage_Temp_Policy_Transaction.tax_loc,
		LKP_Coverage_Temp_Policy_Transaction.class_code,
		LKP_Coverage_Temp_Policy_Transaction.exposure,
		LKP_Coverage_Temp_Policy_Transaction.sub_line_code,
		LKP_Coverage_Temp_Policy_Transaction.source_sar_asl,
		LKP_Coverage_Temp_Policy_Transaction.source_sar_prdct_line,
		LKP_Coverage_Temp_Policy_Transaction.source_sar_sp_use_code,
		LKP_Coverage_Temp_Policy_Transaction.source_statistical_code,
		LKP_Coverage_Temp_Policy_Transaction.section_code,
		LKP_Coverage_Temp_Policy_Transaction.rsn_amended_code,
		LKP_Coverage_Temp_Policy_Transaction.part_code,
		LKP_Coverage_Temp_Policy_Transaction.rating_date_ind,
		EXP_Values.Claim_loss_date
		FROM EXP_Values
		LEFT JOIN LKP_Coverage_Temp_Policy_Transaction
		ON LKP_Coverage_Temp_Policy_Transaction.pol_ak_id = EXP_Values.pol_ak_id AND LKP_Coverage_Temp_Policy_Transaction.sar_id = EXP_Values.loss_id AND LKP_Coverage_Temp_Policy_Transaction.ins_line = EXP_Values.ins_line_out AND LKP_Coverage_Temp_Policy_Transaction.loc_unit_num = EXP_Values.loc_unit_num1 AND LKP_Coverage_Temp_Policy_Transaction.sub_loc_unit_num = EXP_Values.sub_loc_unit_num1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp = EXP_Values.risk_unit_grp1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_First_2pos = EXP_Values.risk_unit_grp_seq_num_First_2pos1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_grp_seq_num_lastpos = EXP_Values.risk_unit_grp_seq_num_last_pos1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit = EXP_Values.risk_unit_complete1 AND LKP_Coverage_Temp_Policy_Transaction.risk_unit_seq_num = EXP_Values.risk_unit_seq_num1 AND LKP_Coverage_Temp_Policy_Transaction.major_peril_code = EXP_Values.major_peril_code1 AND LKP_Coverage_Temp_Policy_Transaction.major_peril_seq_num = EXP_Values.major_peril_seq1 AND LKP_Coverage_Temp_Policy_Transaction.cov_eff_date <= EXP_Values.Claim_loss_date
	),
	OUTPUT AS (
		SELECT
		cov_ak_id AS cov_ak_id_Out, 
		pol_ak_id AS pol_ak_id_Out, 
		temp_pol_trans_ak_id, 
		sar_id AS loss_id_Out, 
		ins_line AS ins_line_Out, 
		loc_unit_num AS loc_unit_num_Out, 
		sub_loc_unit_num AS sub_loc_unit_num_Out, 
		risk_unit_grp AS risk_unit_grp_Out, 
		risk_unit_grp_seq_num_First_2pos AS risk_unit_grp_seq_num_First_2pos_Out, 
		risk_unit_grp_seq_num_lastpos AS risk_unit_grp_seq_num_last_pos_Out, 
		risk_unit AS risk_unit_complete_Out, 
		risk_unit_seq_num AS risk_unit_seq_num_Out, 
		major_peril_code AS major_peril_code_Out, 
		major_peril_seq_num AS major_peril_seq_Out, 
		pms_type_exposure AS pms_type_exposure_Out, 
		cov_eff_date AS cov_eff_date_Out, 
		type_bureau_code AS type_bureau_code_Out, 
		risk_state_prov_code AS risk_state_prov_code_Out, 
		risk_zip_code AS risk_zip_code_Out, 
		terr_code AS terr_code_Out, 
		tax_loc AS tax_loc_Out, 
		class_code AS class_code_Out, 
		exposure AS exposure_Out, 
		sub_line_code AS sub_line_code_Out, 
		source_sar_asl AS source_sar_asl_Out, 
		source_sar_prdct_line AS source_sar_prdct_line_Out, 
		source_sar_sp_use_code, 
		source_statistical_code AS source_statistical_code_Out, 
		source_statistical_line AS source_statistical_line_Out, 
		section_code AS section_code_Out, 
		rsn_amended_code AS rsn_amended_code_Out, 
		part_code AS part_code_Out, 
		rating_date_ind, 
		Claim_loss_date AS Claim_loss_date_Out
		FROM EXP_Lkp_Values
	),
),
EXP_Derive_Values AS (
	SELECT
	FIL_Claim_Transaction_Rows.claim_party_occurrence_ak_id,
	FIL_Claim_Transaction_Rows.claim_occurrence_ak_id,
	FIL_Claim_Transaction_Rows.Claimant_cov_det_ak_id AS claimant_cov_det_ak_id,
	FIL_Claim_Transaction_Rows.claim_pay_ak_id,
	FIL_Claim_Transaction_Rows.claim_trans_id,
	FIL_Claim_Transaction_Rows.claim_trans_ak_id,
	FIL_Claim_Transaction_Rows.claim_rep_ak_id_H,
	FIL_Claim_Transaction_Rows.claim_rep_ak_id_E,
	FIL_Claim_Transaction_Rows.claim_case_ak_id,
	-- *INF*: IIF(ISNULL(claim_case_ak_id),-1,claim_case_ak_id)
	IFF(claim_case_ak_id IS NULL, - 1, claim_case_ak_id) AS claim_case_ak_id_out,
	FIL_Claim_Transaction_Rows.wc_claimant_det_ak_id,
	-- *INF*: IIF(ISNULL(wc_claimant_det_ak_id),-1,wc_claimant_det_ak_id)
	IFF(wc_claimant_det_ak_id IS NULL, - 1, wc_claimant_det_ak_id) AS wc_claimant_det_ak_id_out,
	FIL_Claim_Transaction_Rows.pol_ak_id,
	FIL_Claim_Transaction_Rows.agency_ak_id,
	FIL_Claim_Transaction_Rows.claim_party_ak_id,
	FIL_Claim_Transaction_Rows.contract_cust_ak_id,
	FIL_Claim_Transaction_Rows.claim_occurrence_key,
	FIL_Claim_Transaction_Rows.Policy_key,
	FIL_Claim_Transaction_Rows.Claim_loss_date,
	FIL_Claim_Transaction_Rows.Policy_Symbol,
	FIL_Claim_Transaction_Rows.Policy_Number,
	FIL_Claim_Transaction_Rows.Policy_Module,
	FIL_Claim_Transaction_Rows.loc_unit_num,
	FIL_Claim_Transaction_Rows.loc_unit_num_out,
	FIL_Claim_Transaction_Rows.sub_loc_unit_num,
	FIL_Claim_Transaction_Rows.sub_loc_unit_num_out,
	FIL_Claim_Transaction_Rows.ins_line,
	FIL_Claim_Transaction_Rows.ins_line_out,
	FIL_Claim_Transaction_Rows.risk_unit_grp,
	FIL_Claim_Transaction_Rows.risk_unit_grp_out,
	FIL_Claim_Transaction_Rows.risk_unit_grp_seq_num,
	FIL_Claim_Transaction_Rows.risk_unit_grp_seq_num_First_2pos,
	FIL_Claim_Transaction_Rows.risk_unit_grp_seq_num_last_pos,
	FIL_Claim_Transaction_Rows.risk_unit,
	-- *INF*: SUBSTR(risk_unit,1,3)
	SUBSTR(risk_unit, 1, 3) AS risk_unit_3pos,
	FIL_Claim_Transaction_Rows.risk_unit_seq_num,
	FIL_Claim_Transaction_Rows.risk_unit_seq_num_out,
	-- *INF*: SUBSTR(risk_unit_seq_num_out,1,1)
	SUBSTR(risk_unit_seq_num_out, 1, 1) AS risk_unit_seq_num_first_pos,
	-- *INF*: SUBSTR(risk_unit_seq_num_out,2,1)
	SUBSTR(risk_unit_seq_num_out, 2, 1) AS risk_unit_seq_num_second_pos,
	FIL_Claim_Transaction_Rows.major_peril_code,
	FIL_Claim_Transaction_Rows.major_peril_code_Out,
	FIL_Claim_Transaction_Rows.major_peril_seq,
	FIL_Claim_Transaction_Rows.major_peril_seq_out,
	FIL_Claim_Transaction_Rows.pms_loss_disability,
	FIL_Claim_Transaction_Rows.reserve_ctgry,
	FIL_Claim_Transaction_Rows.cause_of_loss,
	FIL_Claim_Transaction_Rows.pms_type_exposure,
	FIL_Claim_Transaction_Rows.pms_type_exposure_out,
	FIL_Claim_Transaction_Rows.pms_type_bureau_code,
	FIL_Claim_Transaction_Rows.sar_id,
	FIL_Claim_Transaction_Rows.financial_type_code,
	FIL_Claim_Transaction_Rows.pms_trans_code,
	FIL_Claim_Transaction_Rows.trans_code,
	FIL_Claim_Transaction_Rows.trans_date,
	FIL_Claim_Transaction_Rows.pms_acct_entered_date,
	FIL_Claim_Transaction_Rows.trans_base_type_code,
	FIL_Claim_Transaction_Rows.trans_ctgry_code,
	FIL_Claim_Transaction_Rows.Transaction_Account_Date,
	FIL_Claim_Transaction_Rows.trans_amt,
	FIL_Claim_Transaction_Rows.trans_hist_amt,
	FIL_Claim_Transaction_Rows.PMS_Account_Date_To_Check,
	FIL_Claim_Transaction_Rows.LM_Variation_Code,
	FIL_Claim_Transaction_Rows.LM_Amount_OutStanding,
	FIL_Claim_Transaction_Rows.LM_Amount_Paid_Losses,
	FIL_Claim_Transaction_Rows.LM_Amount_Paid_Expenses,
	FIL_Claim_Transaction_Rows.LM_Unpaid_Loss_Adj_Exp,
	-- *INF*: IIF(NOT ISNULL(temp_pol_trans_ak_id),'VALID','NOTVALID')
	IFF(temp_pol_trans_ak_id IS NOT NULL, 'VALID', 'NOTVALID') AS V_Valid_sar_transaction,
	-- *INF*: IIF(V_Valid_sar_transaction = 'VALID',
	-- IIF(LM_Amount_OutStanding = 0.0 AND LM_Unpaid_Loss_Adj_Exp = 0.0 AND LM_Amount_Paid_Losses=0.0 AND LM_Amount_Paid_Expenses=0.0, 'FILTER','NOFILTER'),'FILTER')
	IFF(
	    V_Valid_sar_transaction = 'VALID',
	    IFF(
	        LM_Amount_OutStanding = 0.0
	        and LM_Unpaid_Loss_Adj_Exp = 0.0
	        and LM_Amount_Paid_Losses = 0.0
	        and LM_Amount_Paid_Expenses = 0.0,
	        'FILTER',
	        'NOFILTER'
	    ),
	    'FILTER'
	) AS V_Transaction_Filter,
	V_Transaction_Filter AS Transaction_Filter,
	FIL_Claim_Transaction_Rows.s3p_Claim_Num,
	FIL_Claim_Transaction_Rows.source_claim_occurrence_status_code,
	FIL_Claim_Transaction_Rows.pol_eff_date,
	FIL_Claim_Transaction_Rows.pol_exp_date,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.cov_ak_id_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.temp_pol_trans_ak_id,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.cov_eff_date_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.type_bureau_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.risk_state_prov_code_Out AS risk_state_prov_code,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),StateProvinceCode,risk_state_prov_code)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), StateProvinceCode, risk_state_prov_code) AS risk_state_prov_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.risk_zip_code_Out AS risk_zip_code,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),ZipPostalCode,risk_zip_code)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), ZipPostalCode, risk_zip_code) AS risk_zip_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.terr_code_Out AS terr_code,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),RiskTerritory,terr_code)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), RiskTerritory, terr_code) AS terr_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.tax_loc_Out AS tax_loc,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),TaxLocation,tax_loc)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), TaxLocation, tax_loc) AS tax_loc_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.class_code_Out AS class_code,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),ClassCode,class_code)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), ClassCode, class_code) AS class_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.exposure_Out AS IN_exposure,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),Exposure,IN_exposure)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), Exposure, IN_exposure) AS exposure_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.sub_line_code_Out AS sub_line_code,
	-- *INF*: IIF(IN(PolicySourceID,'PDC', 'DUC'),SublineCode,sub_line_code)
	-- 
	-- ---- For DuckCreek Policies, we are getting the information from RiskLocation.
	IFF(PolicySourceID IN ('PDC','DUC'), SublineCode, sub_line_code) AS sub_line_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.source_sar_asl_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.source_sar_prdct_line_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.source_sar_sp_use_code,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.source_statistical_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.source_statistical_line_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.section_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.rsn_amended_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.part_code_Out,
	mplt_Coverage_Temp_Policy_Transaction_Attributes.rating_date_ind,
	-- *INF*: GET_DATE_PART(trans_date,'MONTH')
	DATE_PART(trans_date, 'MONTH') AS Trans_Date_Month,
	-- *INF*: GET_DATE_PART(trans_date,'DD')
	DATE_PART(trans_date, 'DD') AS Trans_Date_Day,
	-- *INF*: GET_DATE_PART(trans_date,'YYYY')
	DATE_PART(trans_date, 'YYYY') AS Trans_Date_Year,
	-- *INF*: DECODE(TRUE,IN( type_bureau_code_Out,'AL','LP','AI','LI','RL'), '100',
	-- IN( type_bureau_code_Out,'GS','GM','RG'),'400',
	-- IN( type_bureau_code_Out,'WC','WP'),'500',
	-- IN( type_bureau_code_Out,'GL','GI','GN','RQ'),'600',
	-- IN( type_bureau_code_Out,'FF','FM','BF','BP','FT','FP'),'711',
	-- IN( type_bureau_code_Out,'BD'),'722',
	-- IN( type_bureau_code_Out,'BI','BT','RB'),'800')
	DECODE(
	    TRUE,
	    type_bureau_code_Out IN ('AL','LP','AI','LI','RL'), '100',
	    type_bureau_code_Out IN ('GS','GM','RG'), '400',
	    type_bureau_code_Out IN ('WC','WP'), '500',
	    type_bureau_code_Out IN ('GL','GI','GN','RQ'), '600',
	    type_bureau_code_Out IN ('FF','FM','BF','BP','FT','FP'), '711',
	    type_bureau_code_Out IN ('BD'), '722',
	    type_bureau_code_Out IN ('BI','BT','RB'), '800'
	) AS V_Statistical_Line,
	-- *INF*: IIF(ISNULL(V_Statistical_Line),'N/A',V_Statistical_Line)
	IFF(V_Statistical_Line IS NULL, 'N/A', V_Statistical_Line) AS Statistical_Line,
	FIL_Claim_Transaction_Rows.cost_containment_saving_amt,
	-- *INF*: IIF(rating_date_ind = 'C', cov_eff_date_Out , pol_eff_date)
	IFF(rating_date_ind = 'C', cov_eff_date_Out, pol_eff_date) AS v_inception_Date,
	v_inception_Date AS inception_Date,
	-- *INF*: DECODE(TRUE, 
	-- IN(risk_state_prov_code,'60','61','62','63','64','65','66','67','68','69','70','71','72','73','74','75','76','77','78','79','80') AND pms_trans_code = '25',1,
	-- IN(pms_trans_code, '22','42'),1,
	-- Transaction_Account_Date = PMS_Account_Date_To_Check AND IN(pms_trans_code, '90','92','23'),1,
	-- Transaction_Account_Date = PMS_Account_Date_To_Check AND pms_trans_code='41',-1,
	-- 0)
	DECODE(
	    TRUE,
	    risk_state_prov_code IN ('60','61','62','63','64','65','66','67','68','69','70','71','72','73','74','75','76','77','78','79','80') AND pms_trans_code = '25', 1,
	    pms_trans_code IN ('22','42'), 1,
	    Transaction_Account_Date = PMS_Account_Date_To_Check AND pms_trans_code IN ('90','92','23'), 1,
	    Transaction_Account_Date = PMS_Account_Date_To_Check AND pms_trans_code = '41', - 1,
	    0
	) AS v_new_claim_count,
	v_new_claim_count AS new_claim_count,
	FIL_Claim_Transaction_Rows.Loss_Master_Run_Date,
	FIL_Claim_Transaction_Rows.trans_offset_onset_ind,
	FIL_Claim_Transaction_Rows.StatisticalCoverageAKID AS i_StatisticalCoverageAKID,
	-- *INF*: IIF(ISNULL(i_StatisticalCoverageAKID), -1, i_StatisticalCoverageAKID)
	IFF(i_StatisticalCoverageAKID IS NULL, - 1, i_StatisticalCoverageAKID) AS o_StatisticalCoverageAKID,
	FIL_Claim_Transaction_Rows.RatingCoverageAKID AS i_RatingCoverageAKID,
	-- *INF*: IIF(ISNULL(i_RatingCoverageAKID), -1, i_RatingCoverageAKID)
	IFF(i_RatingCoverageAKID IS NULL, - 1, i_RatingCoverageAKID) AS o_RatingCoverageAKID,
	FIL_Claim_Transaction_Rows.PolicySourceID,
	FIL_Claim_Transaction_Rows.ClassCode,
	FIL_Claim_Transaction_Rows.SublineCode,
	FIL_Claim_Transaction_Rows.PolicyCoverageAKID,
	FIL_Claim_Transaction_Rows.RiskLocationAKID,
	FIL_Claim_Transaction_Rows.PremiumTransactionAKID,
	FIL_Claim_Transaction_Rows.BureauStatisticalCodeAKID,
	FIL_Claim_Transaction_Rows.RiskTerritory,
	FIL_Claim_Transaction_Rows.StateProvinceCode,
	FIL_Claim_Transaction_Rows.ZipPostalCode,
	FIL_Claim_Transaction_Rows.TaxLocation,
	FIL_Claim_Transaction_Rows.Exposure,
	FIL_Claim_Transaction_Rows.BureauStatisticalcode1_15,
	FIL_Claim_Transaction_Rows.BureauSpecialUseCode,
	FIL_Claim_Transaction_Rows.PMSAnnualStatementLine,
	FIL_Claim_Transaction_Rows.RatingDateIndicator,
	FIL_Claim_Transaction_Rows.BureauStatisticalUserLine,
	FIL_Claim_Transaction_Rows.AuditReinstatementIndicator
	FROM FIL_Claim_Transaction_Rows
	 -- Manually join with mplt_Coverage_Temp_Policy_Transaction_Attributes
),
SQ_reinsurance_coverage AS (
	SELECT RC.reins_cov_ak_id,
	       RC.pol_ak_id,
	       RC.reins_ins_line,
	       RC.reins_loc_unit_num,
	       RC.reins_sub_loc_unit_num,
	       RC.reins_risk_unit_grp,
	       RC.reins_risk_unit_grp_seq_num,
	       RC.reins_risk_unit,
	       RC.reins_risk_unit_seq_num,
	       RC.reins_section_code,
	       RC.reins_co_num,
	       RC.reins_co_name,
	       RC.reins_eff_date,
	       RC.reins_exp_date,
	       RC.reins_enter_date,
	       RC.reins_type,
	       RC.reins_prcnt_loss_ceded,
	       RC.reins_prcnt_facultative_commssn
	FROM   @{pipeline().parameters.TARGET_TABLE_OWNER}.reinsurance_coverage RC
	WHERE  RC.crrnt_snpsht_flag = 1
),
EXP_Reins_Default AS (
	SELECT
	reins_cov_ak_id,
	pol_ak_id,
	reins_ins_line,
	reins_loc_unit_num,
	reins_sub_loc_unit_num,
	reins_risk_unit_grp,
	reins_risk_unit_grp_seq_num,
	reins_risk_unit,
	reins_risk_unit_seq_num,
	reins_section_code,
	reins_co_num,
	reins_co_name,
	reins_eff_date,
	reins_exp_date,
	reins_enter_date,
	reins_type,
	reins_prcnt_loss_ceded,
	reins_prcnt_facultative_commssn
	FROM SQ_reinsurance_coverage
),
JNR_Reinsurance_Coverage_n_Transaction AS (SELECT
	EXP_Reins_Default.reins_cov_ak_id, 
	EXP_Reins_Default.pol_ak_id, 
	EXP_Reins_Default.reins_ins_line, 
	EXP_Reins_Default.reins_loc_unit_num, 
	EXP_Reins_Default.reins_sub_loc_unit_num, 
	EXP_Reins_Default.reins_risk_unit_grp, 
	EXP_Reins_Default.reins_risk_unit_grp_seq_num, 
	EXP_Reins_Default.reins_risk_unit, 
	EXP_Reins_Default.reins_risk_unit_seq_num, 
	EXP_Reins_Default.reins_section_code, 
	EXP_Reins_Default.reins_co_num, 
	EXP_Reins_Default.reins_co_name, 
	EXP_Reins_Default.reins_eff_date, 
	EXP_Reins_Default.reins_exp_date, 
	EXP_Reins_Default.reins_enter_date, 
	EXP_Reins_Default.reins_type, 
	EXP_Reins_Default.reins_prcnt_loss_ceded, 
	EXP_Reins_Default.reins_prcnt_facultative_commssn, 
	EXP_Derive_Values.pol_ak_id AS pol_ak_id_out, 
	EXP_Derive_Values.section_code_Out, 
	EXP_Derive_Values.sar_id, 
	EXP_Derive_Values.ins_line_out, 
	EXP_Derive_Values.loc_unit_num_out, 
	EXP_Derive_Values.sub_loc_unit_num_out, 
	EXP_Derive_Values.risk_unit_grp_out, 
	EXP_Derive_Values.risk_unit_grp_seq_num, 
	EXP_Derive_Values.risk_unit_grp_seq_num_First_2pos, 
	EXP_Derive_Values.risk_unit_grp_seq_num_last_pos, 
	EXP_Derive_Values.risk_unit, 
	EXP_Derive_Values.risk_unit_complete, 
	EXP_Derive_Values.risk_unit_seq_num_out, 
	EXP_Derive_Values.risk_unit_seq_num_first_pos, 
	EXP_Derive_Values.risk_unit_seq_num_second_pos, 
	EXP_Derive_Values.major_peril_code_Out, 
	EXP_Derive_Values.major_peril_seq_out, 
	EXP_Derive_Values.rsn_amended_code_Out, 
	EXP_Derive_Values.trans_amt, 
	EXP_Derive_Values.trans_hist_amt, 
	EXP_Derive_Values.PMS_Account_Date_To_Check, 
	EXP_Derive_Values.Claim_loss_date, 
	EXP_Derive_Values.pms_trans_code, 
	EXP_Derive_Values.risk_unit_3pos, 
	EXP_Derive_Values.Policy_Symbol, 
	EXP_Derive_Values.Policy_Number, 
	EXP_Derive_Values.Policy_Module, 
	EXP_Derive_Values.type_bureau_code_Out, 
	EXP_Derive_Values.risk_state_prov_code_Out, 
	EXP_Derive_Values.risk_zip_code_Out, 
	EXP_Derive_Values.terr_code_Out, 
	EXP_Derive_Values.tax_loc_Out, 
	EXP_Derive_Values.class_code_Out, 
	EXP_Derive_Values.exposure_Out, 
	EXP_Derive_Values.sub_line_code_Out, 
	EXP_Derive_Values.source_sar_asl_Out, 
	EXP_Derive_Values.source_sar_prdct_line_Out, 
	EXP_Derive_Values.source_sar_sp_use_code, 
	EXP_Derive_Values.source_statistical_code_Out, 
	EXP_Derive_Values.claim_trans_id, 
	EXP_Derive_Values.claim_trans_ak_id, 
	EXP_Derive_Values.claimant_cov_det_ak_id, 
	EXP_Derive_Values.claim_rep_ak_id_H, 
	EXP_Derive_Values.claim_rep_ak_id_E, 
	EXP_Derive_Values.cov_ak_id_Out, 
	EXP_Derive_Values.pol_ak_id AS pol_ak_id_Out1, 
	EXP_Derive_Values.agency_ak_id, 
	EXP_Derive_Values.contract_cust_ak_id, 
	EXP_Derive_Values.temp_pol_trans_ak_id, 
	EXP_Derive_Values.claim_party_occurrence_ak_id, 
	EXP_Derive_Values.claim_party_ak_id, 
	EXP_Derive_Values.claim_occurrence_ak_id, 
	EXP_Derive_Values.claim_pay_ak_id, 
	EXP_Derive_Values.claim_case_ak_id_out AS claim_case_ak_id, 
	EXP_Derive_Values.wc_claimant_det_ak_id_out AS wc_claimant_det_ak_id, 
	EXP_Derive_Values.Statistical_Line, 
	EXP_Derive_Values.inception_Date, 
	EXP_Derive_Values.new_claim_count, 
	EXP_Derive_Values.Loss_Master_Run_Date, 
	EXP_Derive_Values.LM_Variation_Code, 
	EXP_Derive_Values.LM_Amount_OutStanding, 
	EXP_Derive_Values.LM_Amount_Paid_Losses, 
	EXP_Derive_Values.LM_Amount_Paid_Expenses, 
	EXP_Derive_Values.LM_Unpaid_Loss_Adj_Exp, 
	EXP_Derive_Values.trans_date, 
	EXP_Derive_Values.pms_acct_entered_date, 
	EXP_Derive_Values.trans_offset_onset_ind, 
	EXP_Derive_Values.o_StatisticalCoverageAKID AS StatisticalCoverageAKID, 
	EXP_Derive_Values.o_RatingCoverageAKID AS RatingCoverageAKID, 
	EXP_Derive_Values.financial_type_code, 
	EXP_Derive_Values.trans_code, 
	EXP_Derive_Values.PolicyCoverageAKID, 
	EXP_Derive_Values.RiskLocationAKID, 
	EXP_Derive_Values.PremiumTransactionAKID, 
	EXP_Derive_Values.BureauStatisticalCodeAKID
	FROM EXP_Derive_Values
	INNER JOIN EXP_Reins_Default
	ON EXP_Reins_Default.pol_ak_id = EXP_Derive_Values.pol_ak_id AND EXP_Reins_Default.reins_section_code = EXP_Derive_Values.section_code_Out
),
EXP_Transform_Statistical_Codes AS (
	SELECT
	source_statistical_code_Out AS statistical_code,
	major_peril_code_Out AS major_peril,
	type_bureau_code_Out AS Type_Bureau,
	class_code_Out AS sar_class_code,
	-- *INF*: statistical_code
	-- 
	-- --DECODE(TRUE, Type_Bureau = 'BE', ' '  || statistical_code,
	-- --Type_Bureau = 'BF', ' '  || statistical_code,
	-- --Type_Bureau = 'RP' AND major_peril = '145', ' '  || statistical_code,
	-- --Type_Bureau = 'RL' AND major_peril = '114', '  '  || statistical_code,
	-- --Type_Bureau = 'RL' AND major_peril = '119', '     '  || statistical_code,
	-- --statistical_code)
	-- 
	-- ---- Had to introduce space at the begining of the field because of LTRIM(RTRIM)) to statistical codes in Temp_Policy_transaction Table.
	statistical_code AS v_statistical_code,
	'D' AS v_stat_plan_id,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,1,1))=0,' ',SUBSTR(v_statistical_code,1,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 1, 1)) = 0, ' ', SUBSTR(v_statistical_code, 1, 1)) AS v_pos_1,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,2,1))=0,' ',SUBSTR(v_statistical_code,2,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 2, 1)) = 0, ' ', SUBSTR(v_statistical_code, 2, 1)) AS v_pos_2,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,3,1))=0,' ',SUBSTR(v_statistical_code,3,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 3, 1)) = 0, ' ', SUBSTR(v_statistical_code, 3, 1)) AS v_pos_3,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,4,1))=0,' ',SUBSTR(v_statistical_code,4,1))
	-- 
	-- ----Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 4, 1)) = 0, ' ', SUBSTR(v_statistical_code, 4, 1)) AS v_pos_4,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,5,1))=0,' ',SUBSTR(v_statistical_code,5,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 5, 1)) = 0, ' ', SUBSTR(v_statistical_code, 5, 1)) AS v_pos_5,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,6,1))=0,' ',SUBSTR(v_statistical_code,6,1))
	-- 
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 6, 1)) = 0, ' ', SUBSTR(v_statistical_code, 6, 1)) AS v_pos_6,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,7,1))=0,' ',SUBSTR(v_statistical_code,7,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 7, 1)) = 0, ' ', SUBSTR(v_statistical_code, 7, 1)) AS v_pos_7,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,8,1))=0,' ',SUBSTR(v_statistical_code,8,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 8, 1)) = 0, ' ', SUBSTR(v_statistical_code, 8, 1)) AS v_pos_8,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,9,1))=0,' ',SUBSTR(v_statistical_code,9,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 9, 1)) = 0, ' ', SUBSTR(v_statistical_code, 9, 1)) AS v_pos_9,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,10,1))=0,' ',SUBSTR(v_statistical_code,10,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 10, 1)) = 0, ' ', SUBSTR(v_statistical_code, 10, 1)) AS v_pos_10,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,11,1))=0,' ',SUBSTR(v_statistical_code,11,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 11, 1)) = 0, ' ', SUBSTR(v_statistical_code, 11, 1)) AS v_pos_11,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,12,1))=0,' ',SUBSTR(v_statistical_code,12,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 12, 1)) = 0, ' ', SUBSTR(v_statistical_code, 12, 1)) AS v_pos_12,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,13,1))=0,' ',SUBSTR(v_statistical_code,13,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 13, 1)) = 0, ' ', SUBSTR(v_statistical_code, 13, 1)) AS v_pos_13,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,14,1))=0,' ',SUBSTR(v_statistical_code,14,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 14, 1)) = 0, ' ', SUBSTR(v_statistical_code, 14, 1)) AS v_pos_14,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,15,1))=0,' ',SUBSTR(v_statistical_code,15,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 15, 1)) = 0, ' ', SUBSTR(v_statistical_code, 15, 1)) AS v_pos_15,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,16,1))=0,' ',SUBSTR(v_statistical_code,16,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 16, 1)) = 0, ' ', SUBSTR(v_statistical_code, 16, 1)) AS v_pos_16,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,17,1))=0,' ',SUBSTR(v_statistical_code,17,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 17, 1)) = 0, ' ', SUBSTR(v_statistical_code, 17, 1)) AS v_pos_17,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,18,1))=0,' ',SUBSTR(v_statistical_code,18,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 18, 1)) = 0, ' ', SUBSTR(v_statistical_code, 18, 1)) AS v_pos_18,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,19,1))=0,' ',SUBSTR(v_statistical_code,19,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 19, 1)) = 0, ' ', SUBSTR(v_statistical_code, 19, 1)) AS v_pos_19,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,20,1))=0,' ',SUBSTR(v_statistical_code,20,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 20, 1)) = 0, ' ', SUBSTR(v_statistical_code, 20, 1)) AS v_pos_20,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,21,1))=0,' ',SUBSTR(v_statistical_code,21,1))
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,21,1))=0,' ',SUBSTR(v_statistical_code,21,1))
	-- 
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(
	    TRUE,
	    Type_Bureau = 'RP', '0',
	    LENGTH(SUBSTR(v_statistical_code, 21, 1)) = 0, ' ',
	    SUBSTR(v_statistical_code, 21, 1)
	) AS v_pos_21,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,22,1))=0,' ',SUBSTR(v_statistical_code,22,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,22,1))=0,' ',SUBSTR(v_statistical_code,22,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(
	    TRUE,
	    Type_Bureau = 'RP', '0',
	    LENGTH(SUBSTR(v_statistical_code, 22, 1)) = 0, ' ',
	    SUBSTR(v_statistical_code, 22, 1)
	) AS v_pos_22,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,23,1))=0,' ',SUBSTR(v_statistical_code,23,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,23,1))=0,' ',SUBSTR(v_statistical_code,23,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(
	    TRUE,
	    Type_Bureau = 'RP', '0',
	    LENGTH(SUBSTR(v_statistical_code, 23, 1)) = 0, ' ',
	    SUBSTR(v_statistical_code, 23, 1)
	) AS v_pos_23,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','0',
	-- LENGTH(SUBSTR(v_statistical_code,24,1))=0,' ',SUBSTR(v_statistical_code,24,1))
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,24,1))=0,' ',SUBSTR(v_statistical_code,24,1))
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '0'
	DECODE(
	    TRUE,
	    Type_Bureau = 'RP', '0',
	    LENGTH(SUBSTR(v_statistical_code, 24, 1)) = 0, ' ',
	    SUBSTR(v_statistical_code, 24, 1)
	) AS v_pos_24,
	-- *INF*: DECODE(TRUE,Type_Bureau='RP','{',
	-- LENGTH(SUBSTR(v_statistical_code,25,1))=0,' ',SUBSTR(v_statistical_code,25,1))
	-- 
	-- --- IN COBOL "{" represents a  +ve sign and "}" is -ve sign, since this is base rate for Type_Bureau RP is a sign field so COBOL creates "{". Replicating the COBOL logic.
	-- 
	-- --- Statistical Code field is initialised at the begining of the WMM01A0 module to all spaces but since it is a sign field for Type Bureau of RP, these are defaulted to '{'
	-- 
	-- --IIF(LENGTH(SUBSTR(v_statistical_code,25,1))=0,' ',SUBSTR(v_statistical_code,25,1))
	DECODE(
	    TRUE,
	    Type_Bureau = 'RP', '{',
	    LENGTH(SUBSTR(v_statistical_code, 25, 1)) = 0, ' ',
	    SUBSTR(v_statistical_code, 25, 1)
	) AS v_pos_25,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,26,1))=0,' ',SUBSTR(v_statistical_code,26,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 26, 1)) = 0, ' ', SUBSTR(v_statistical_code, 26, 1)) AS v_pos_26,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,27,1))=0,' ',SUBSTR(v_statistical_code,27,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 27, 1)) = 0, ' ', SUBSTR(v_statistical_code, 27, 1)) AS v_pos_27,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,28,1))=0,' ',SUBSTR(v_statistical_code,28,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 28, 1)) = 0, ' ', SUBSTR(v_statistical_code, 28, 1)) AS v_pos_28,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,29,1))=0,' ',SUBSTR(v_statistical_code,29,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 29, 1)) = 0, ' ', SUBSTR(v_statistical_code, 29, 1)) AS v_pos_29,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,30,1))=0,' ',SUBSTR(v_statistical_code,30,1))
	IFF(LENGTH(SUBSTR(v_statistical_code, 30, 1)) = 0, ' ', SUBSTR(v_statistical_code, 30, 1)) AS v_pos_30,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,31,1))=0,' ',SUBSTR(v_statistical_code,31,1))
	-- 
	-- ----8/18/2011 Uma Bollu - Introducing Blank Space intentionally as PIF_4514_Stage has spaces but when we add this data into EDW we do a LTRIM, RTRIM so this Target Lookup finds a match but this Statistical Code calculation we need spaces because of the logic which re-arranges the fields and this is very important for Bureau Reporting etc.
	IFF(LENGTH(SUBSTR(v_statistical_code, 31, 1)) = 0, ' ', SUBSTR(v_statistical_code, 31, 1)) AS v_pos_31,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,32,1))=0,' ',SUBSTR(v_statistical_code,32,1))
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 32, 1)) = 0, ' ', SUBSTR(v_statistical_code, 32, 1)) AS v_pos_32,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,33,1))=0,' ',SUBSTR(v_statistical_code,33,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 33, 1)) = 0, ' ', SUBSTR(v_statistical_code, 33, 1)) AS v_pos_33,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,34,1))=0,' ',SUBSTR(v_statistical_code,34,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 34, 1)) = 0, ' ', SUBSTR(v_statistical_code, 34, 1)) AS v_pos_34,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,35,1))=0,' ',SUBSTR(v_statistical_code,35,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 35, 1)) = 0, ' ', SUBSTR(v_statistical_code, 35, 1)) AS v_pos_35,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,36,1))=0,' ',SUBSTR(v_statistical_code,36,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 36, 1)) = 0, ' ', SUBSTR(v_statistical_code, 36, 1)) AS v_pos_36,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,37,1))=0,' ',SUBSTR(v_statistical_code,37,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 37, 1)) = 0, ' ', SUBSTR(v_statistical_code, 37, 1)) AS v_pos_37,
	-- *INF*: IIF(LENGTH(SUBSTR(v_statistical_code,38,1))=0,' ',SUBSTR(v_statistical_code,38,1))
	-- 
	-- 
	IFF(LENGTH(SUBSTR(v_statistical_code, 38, 1)) = 0, ' ', SUBSTR(v_statistical_code, 38, 1)) AS v_pos_38,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS Generic,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Code_AC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_AI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_26 || '       ' || v_pos_25 || v_pos_23  || v_pos_24  || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22)
	-- 
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_26 || '       ' || v_pos_25 || v_pos_23 || v_pos_24 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22) AS v_Stat_Codes_AL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10  || v_pos_11|| v_pos_20 || v_pos_21  || 
	-- '             ' ||  v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19  )
	-- 
	--  -----It has a Filler of 13 spaces
	-- --- I have checked this code this is fine
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_20 || v_pos_21 || '             ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19) AS v_Stat_Codes_AN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 ||
	-- '      ' || v_pos_14 || v_pos_23  || v_pos_24  || '  '  ||  v_pos_26  || v_pos_27  || v_pos_28  || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || '      ' || v_pos_14 || v_pos_23 || v_pos_24 || '  ' || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22) AS v_Stat_Codes_AP,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_11 || v_pos_10 || 
	--   v_pos_12 || v_pos_13 )
	-- 
	-- --- Verified the logic
	-- 
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_11 || v_pos_10 || v_pos_12 || v_pos_13) AS v_Stat_Codes_A2,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_11 || v_pos_12 )
	-- 
	-- --- Verified logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_11 || v_pos_12) AS v_Stat_Codes_A3,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 ||
	-- '           '  ||  v_pos_22 || v_pos_29 || '  ' || v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28)
	-- 
	-- --- Verified logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || '           ' || v_pos_22 || v_pos_29 || '  ' || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28) AS v_Stat_Codes_BB,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17  || v_pos_20  || v_pos_27  || v_pos_28  || v_pos_29 || '    ' ||v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26 )
	-- 
	-- 
	-- -- Verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_20 || v_pos_27 || v_pos_28 || v_pos_29 || '    ' || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26) AS v_Stat_Codes_BC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- --- Verified logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_BD,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 ||  v_pos_6 || v_pos_7 || '                    ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13)
	-- 
	-- 
	--  ---  Verified Logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || '                    ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13) AS v_Stat_Codes_BE,
	-- *INF*: ('  '  || v_pos_4  || v_pos_5 || ' ' || v_pos_14 || '  ' || v_pos_15 || v_pos_16 || '   ' ||  v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || '     ' || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22 || '   ')
	-- 
	-- 
	-- --8/22/2011 - Added 2 spaces in the beginning. In COBOL, statitistical code field is initialised to spaces at the start of reformatting. If there is no code to move certain fields then the spaces stay as it is except other fileds are layed out over spaces.
	-- --- Verified the logic
	-- 
	('  ' || v_pos_4 || v_pos_5 || ' ' || v_pos_14 || '  ' || v_pos_15 || v_pos_16 || '   ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || '     ' || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || '   ') AS v_Stat_Codes_BF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_4  || v_pos_5)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_4 || v_pos_5) AS v_Stat_Codes_BP,
	-- *INF*: (v_pos_1 || v_pos_2 )
	-- 
	-- --- Verified the logic
	(v_pos_1 || v_pos_2) AS v_Stat_Codes_BI,
	-- *INF*: v_pos_1
	-- 
	-- -- verified the logic
	v_pos_1 AS v_Stat_Codes_BL,
	-- *INF*: (SUBSTR(sar_class_code,1,3) || '  ' || v_pos_18  ||  v_pos_19 || v_pos_1 ||  ' ' ||  v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 
	-- || '    ' ||  v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34 || v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28 || '   ' )
	-- 
	-- --- Verfied the logic
	(SUBSTR(sar_class_code, 1, 3) || '  ' || v_pos_18 || v_pos_19 || v_pos_1 || ' ' || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || '    ' || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || '   ') AS v_Stat_Codes_BM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || '      '  ||  v_pos_8 || v_pos_9 || '           ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19)
	-- 
	--  ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '      ' || v_pos_8 || v_pos_9 || '           ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19) AS v_Stat_Codes_BT,
	-- *INF*: (v_pos_1 || v_pos_2 || '      '  || v_pos_9 || v_pos_10 || v_pos_17 || v_pos_18 ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || '      ' || v_pos_9 || v_pos_10 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31) AS v_Stat_Codes_B2,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17)
	-- 
	-- ----- verified the logic
	-- 
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17) AS v_Stat_Codes_CC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || 
	--  v_pos_17 || v_pos_18  || ' ' ||  v_pos_20 || '              ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_17 || v_pos_18 || ' ' || v_pos_20 || '              ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_CF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- ---- Generic 
	-- -- No Change from Input copybook to Output
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_CR,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_6 || v_pos_7 || ' '  || v_pos_9 || '  ' || v_pos_12 || ' ' || v_pos_14 || v_pos_15 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_6 || v_pos_7 || ' ' || v_pos_9 || '  ' || v_pos_12 || ' ' || v_pos_14 || v_pos_15) AS v_Stat_Codes_CI,
	-- *INF*: (v_pos_1 || v_pos_4  || v_pos_6 || v_pos_7 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_4 || v_pos_6 || v_pos_7) AS v_Stat_Codes_CL,
	-- *INF*: ('  ' || v_pos_1 || v_pos_2 || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- ---- verified the logic
	('  ' || v_pos_1 || v_pos_2 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_CP,
	-- *INF*: (v_pos_3 || v_pos_4  || v_pos_5 )
	-- 
	-- ---- verified the logic
	(v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_CN,
	-- *INF*: v_pos_1
	-- 
	-- -----
	v_pos_1 AS v_Stat_Codes_EI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || '                   ' ||v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16)
	-- 
	-- ---- verified the logic
	-- --- 19 spaces
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_EQ,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4) AS v_Stat_Codes_FC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 
	-- || '                  ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 )
	-- 
	-- ---- verified the logic
	-- ---- 18 Spaces
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '                  ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_FF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_FM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16)
	-- 
	-- ---- verified the logic
	-- --- 19 spaces
	-- 
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || '                   ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16) AS v_Stat_Codes_FO,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3) AS v_Stat_Codes_FP,
	-- *INF*: (v_pos_1 || v_pos_2 || ' ' || v_pos_3 || '  ' || v_pos_6 || v_pos_7 || '   ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 ||
	-- '       ' || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22 || '   ')
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || ' ' || v_pos_3 || '  ' || v_pos_6 || v_pos_7 || '   ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || '       ' || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || '   ') AS v_Stat_Codes_FT,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_12 || v_pos_13 || '                '  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9)
	-- 
	-- ---- verified the logic
	-- -- 17 Spaces
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_12 || v_pos_13 || '                ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9) AS v_Stat_Codes_GI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_4  || v_pos_5  || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || '      ' || v_pos_13 || v_pos_29  || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19 ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_6 || v_pos_7 || v_pos_4 || v_pos_5 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || '      ' || v_pos_13 || v_pos_29 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28) AS v_Stat_Codes_GL,
	-- *INF*: (v_pos_1 || '           '  ||   v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	-- ---- verified the logic
	-- 
	(v_pos_1 || '           ' || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_GP,
	-- *INF*: (v_pos_1 || '                       ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_11 || v_pos_12 || v_pos_13)
	-- 
	-- ---- verified the logic
	-- --- 23 spaces
	-- 
	-- 
	-- 
	(v_pos_1 || '                       ' || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_11 || v_pos_12 || v_pos_13) AS v_Stat_Codes_GS,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_18  ||  v_pos_19  
	-- || ' ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || '                ')
	-- 
	-- 
	-- ---- verified the logic
	-- --- 16 Spaces at the end
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_18 || v_pos_19 || ' ' || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || '                ') AS v_Stat_Codes_HO,
	-- *INF*: ('        ' || v_pos_11 || v_pos_12 || '               '  || v_pos_4  || v_pos_5  || v_pos_6  || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_14 || v_pos_15 || v_pos_17)
	-- 
	-- ---- verified the logic
	('        ' || v_pos_11 || v_pos_12 || '               ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_14 || v_pos_15 || v_pos_17) AS v_Stat_Codes_IM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  || v_pos_24  || v_pos_25  || v_pos_26 || v_pos_28  || v_pos_29  || v_pos_30 || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35) AS v_Stat_Codes_JR,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  )
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_ME,
	-- *INF*: (v_pos_1 || ' '  || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' ||  v_pos_11 || v_pos_12 || v_pos_13 || '  ' || v_pos_18  || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || '                ' ) 
	-- 
	-- --- need logic for stat-plan -id
	-- ---- 16 Spaces at the end
	(v_pos_1 || ' ' || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_10 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || '  ' || v_pos_18 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || '                ') AS v_Stat_Codes_MH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || '                  '  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7)
	-- 
	--  --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || '                  ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7) AS v_Stat_Codes_MI,
	-- *INF*: (v_pos_6 || v_pos_7 || v_pos_3 || v_pos_4  || v_pos_2 || '      ' || v_pos_1 || '        ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || ' ' ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24 )
	-- 
	--  --- verified the logic
	(v_pos_6 || v_pos_7 || v_pos_3 || v_pos_4 || v_pos_2 || '      ' || v_pos_1 || '        ' || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || ' ' || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24) AS v_Stat_Codes_ML,
	-- *INF*: -- No Stats code in the Output Copybook just the policy_type logic
	'' AS v_Stat_Codes_MP,
	-- *INF*: (SUBSTR(sar_class_code,1,3) || v_pos_17 || v_pos_18 ||  v_pos_19  || v_pos_1 || v_pos_2 || v_pos_3 || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || '   ' || '       ' || '      ' )
	-- 
	-- --- Need to look at complete logic
	-- 
	(SUBSTR(sar_class_code, 1, 3) || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_1 || v_pos_2 || v_pos_3 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || '   ' || '       ' || '      ') AS v_Stat_Codes_M2,
	-- *INF*: ( '                 ' || v_stat_plan_id)
	-- 
	-- ----verified the logic
	('                 ' || v_stat_plan_id) AS v_Stat_Codes_NE,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  ||  v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_19)
	-- 
	-- --- Verified the Logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_19) AS v_Stat_Codes_PC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || v_pos_19  || v_pos_20  ||  v_pos_21)
	-- 
	-- --- verified the logic
	--  
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || ' ' || v_pos_19 || v_pos_20 || v_pos_21) AS v_Stat_Codes_PH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_PF,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_PI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_PL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 ||  v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18) AS v_Stat_Codes_PM,
	-- *INF*: (v_pos_1 || v_pos_2)
	-- 
	-- --- verified the logic
	-- 
	(v_pos_1 || v_pos_2) AS v_Stat_Codes_RB,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3) AS v_Stat_Codes_RG,
	-- *INF*: (v_pos_1 || v_pos_2)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2) AS v_Stat_Codes_RI,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24) AS v_Stat_Codes_RL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_8 || v_pos_9 || v_pos_10) AS v_Stat_Codes_RM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || ' ' || 
	-- v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21 || v_pos_22 ||  v_pos_23  || v_pos_24)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || ' ' || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24) AS v_Stat_Codes_RN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29 || v_pos_30 || v_pos_31|| v_pos_33 || v_pos_34  ||  v_pos_35  || v_pos_32)
	-- 
	-- ----
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_32) AS v_Stat_Codes_RP,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5 )
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5) AS v_Stat_Codes_RQ,
	-- *INF*: (v_pos_1 || ' ' || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 )
	-- 
	-- --- verified the logic
	(v_pos_1 || ' ' || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8) AS v_Stat_Codes_SM,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_6 || v_pos_8 || v_pos_11 || v_pos_9)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_6 || v_pos_8 || v_pos_11 || v_pos_9) AS v_Stat_Codes_TH,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 
	-- || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19
	-- ||  v_pos_22  ||  v_pos_23  || v_pos_24 || '       ' || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36)
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_22 || v_pos_23 || v_pos_24 || '       ' || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36) AS v_Stat_Codes_VL,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19 
	--  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30 || ' ' || v_pos_32  ||  v_pos_33
	-- || v_pos_34  ||  v_pos_35  || v_pos_36 )
	-- 
	-- --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || ' ' || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36) AS v_Stat_Codes_VP,
	-- *INF*: ('   ' || v_pos_4  || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12  || ' ' || v_pos_14 || v_pos_15 || '              ' 
	-- || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34 || v_pos_35)
	-- 
	-- --- verified the logic
	('   ' || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || ' ' || v_pos_14 || v_pos_15 || '              ' || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35) AS v_Stat_Codes_VN,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  
	-- || ' ' || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || '    ' || v_pos_36 || v_pos_37  || v_pos_38)
	-- 
	-- ---- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || ' ' || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || '    ' || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Codes_VC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31)
	-- 
	--  --- verified the logic
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31) AS v_Stat_Codes_WC,
	-- *INF*: (v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4  || v_pos_5  || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18  ||  v_pos_19  || v_pos_20  ||  v_pos_21  ||  v_pos_22  ||  v_pos_23  || v_pos_24  || v_pos_25  || v_pos_26  || v_pos_27  || v_pos_28  || v_pos_29  || v_pos_30  || v_pos_31 || v_pos_32  ||  v_pos_33  || v_pos_34  ||  v_pos_35  || v_pos_36 || v_pos_37  || v_pos_38)
	(v_pos_1 || v_pos_2 || v_pos_3 || v_pos_4 || v_pos_5 || v_pos_6 || v_pos_7 || v_pos_8 || v_pos_9 || v_pos_10 || v_pos_11 || v_pos_12 || v_pos_13 || v_pos_14 || v_pos_15 || v_pos_16 || v_pos_17 || v_pos_18 || v_pos_19 || v_pos_20 || v_pos_21 || v_pos_22 || v_pos_23 || v_pos_24 || v_pos_25 || v_pos_26 || v_pos_27 || v_pos_28 || v_pos_29 || v_pos_30 || v_pos_31 || v_pos_32 || v_pos_33 || v_pos_34 || v_pos_35 || v_pos_36 || v_pos_37 || v_pos_38) AS v_Stat_Code_WP,
	-- *INF*: ('   ' || v_pos_1 || v_pos_2 || '            ' || v_stat_plan_id)
	-- 
	-- --8/19/2011 Added v_stat_plan_id
	-- --- need to bring stat plan_id
	--  --- verified the logic but need stat plan id
	-- 
	('   ' || v_pos_1 || v_pos_2 || '            ' || v_stat_plan_id) AS v_Stat_Codes_WL,
	-- *INF*: DECODE(Type_Bureau, 'AC', v_Stat_Code_AC, 'AI', v_Stat_Codes_AI, 'AL', v_Stat_Codes_AL, 'AN', v_Stat_Codes_AN, 'AP', v_Stat_Codes_AP, 'A2', v_Stat_Codes_A2, 'A3', v_Stat_Codes_A3, 'BB', v_Stat_Codes_BB, 'BC', v_Stat_Codes_BC, 'BD', v_Stat_Codes_BD, 'BE', v_Stat_Codes_BE, 'BF', v_Stat_Codes_BF, 'BP', v_Stat_Codes_BP, 'BI', v_Stat_Codes_BI, 'BL', v_Stat_Codes_BL, 'BM', v_Stat_Codes_BM, 'BT', v_Stat_Codes_BT, 'B2', v_Stat_Codes_B2, 'CC', v_Stat_Codes_CC, 'CF', v_Stat_Codes_CF, 'CI', v_Stat_Codes_CI, 'CL', v_Stat_Codes_CL, 'CN', v_Stat_Codes_CN, 'CP', v_Stat_Codes_CP, 'EI', v_Stat_Codes_EI, 'EQ', v_Stat_Codes_EQ, 'FC', v_Stat_Codes_FC, 'FF', v_Stat_Codes_FF, 'FM', v_Stat_Codes_FM, 'FO', v_Stat_Codes_FO, 'FP', v_Stat_Codes_FP, 'FT', v_Stat_Codes_FT, 'GI', v_Stat_Codes_GI, 'GL', v_Stat_Codes_GL, 'GP', v_Stat_Codes_GP, 'GS', v_Stat_Codes_GS, 'HO', v_Stat_Codes_HO, 'IM', v_Stat_Codes_IM, 'JR', v_Stat_Codes_JR, 'ME', v_Stat_Codes_ME, 'MH', v_Stat_Codes_MH, 'MI', v_Stat_Codes_MI, 'ML',
	-- v_Stat_Codes_ML, 'MP', v_Stat_Codes_MP, 'M2', v_Stat_Codes_M2, 'NE', v_Stat_Codes_NE, 'PC', v_Stat_Codes_PC, 'PH', v_Stat_Codes_PH, 'PM', v_Stat_Codes_PM, 'RB', v_Stat_Codes_RB, 'RG', v_Stat_Codes_RG, 'RI', v_Stat_Codes_RI, 'RL', v_Stat_Codes_RL, 'RM', v_Stat_Codes_RM, 'RN', v_Stat_Codes_RN, 'RP', v_Stat_Codes_RP, 'RQ', v_Stat_Codes_RQ, 'SM', v_Stat_Codes_SM, 'TH', v_Stat_Codes_TH, 'VL', v_Stat_Codes_VL, 'VP', v_Stat_Codes_VP, 'VN', v_Stat_Codes_VN, 'VC', v_Stat_Codes_VC, 'WC', v_Stat_Codes_WC, 'WL', v_Stat_Codes_WL,
	-- 'CR', v_Stat_Code_CR, 'PF', v_Stat_Code_PF,'PI', v_Stat_Code_PI, 'PL', v_Stat_Code_PL,
	-- 'WP', v_Stat_Code_WP,v_statistical_code) 
	DECODE(
	    Type_Bureau,
	    'AC', v_Stat_Code_AC,
	    'AI', v_Stat_Codes_AI,
	    'AL', v_Stat_Codes_AL,
	    'AN', v_Stat_Codes_AN,
	    'AP', v_Stat_Codes_AP,
	    'A2', v_Stat_Codes_A2,
	    'A3', v_Stat_Codes_A3,
	    'BB', v_Stat_Codes_BB,
	    'BC', v_Stat_Codes_BC,
	    'BD', v_Stat_Codes_BD,
	    'BE', v_Stat_Codes_BE,
	    'BF', v_Stat_Codes_BF,
	    'BP', v_Stat_Codes_BP,
	    'BI', v_Stat_Codes_BI,
	    'BL', v_Stat_Codes_BL,
	    'BM', v_Stat_Codes_BM,
	    'BT', v_Stat_Codes_BT,
	    'B2', v_Stat_Codes_B2,
	    'CC', v_Stat_Codes_CC,
	    'CF', v_Stat_Codes_CF,
	    'CI', v_Stat_Codes_CI,
	    'CL', v_Stat_Codes_CL,
	    'CN', v_Stat_Codes_CN,
	    'CP', v_Stat_Codes_CP,
	    'EI', v_Stat_Codes_EI,
	    'EQ', v_Stat_Codes_EQ,
	    'FC', v_Stat_Codes_FC,
	    'FF', v_Stat_Codes_FF,
	    'FM', v_Stat_Codes_FM,
	    'FO', v_Stat_Codes_FO,
	    'FP', v_Stat_Codes_FP,
	    'FT', v_Stat_Codes_FT,
	    'GI', v_Stat_Codes_GI,
	    'GL', v_Stat_Codes_GL,
	    'GP', v_Stat_Codes_GP,
	    'GS', v_Stat_Codes_GS,
	    'HO', v_Stat_Codes_HO,
	    'IM', v_Stat_Codes_IM,
	    'JR', v_Stat_Codes_JR,
	    'ME', v_Stat_Codes_ME,
	    'MH', v_Stat_Codes_MH,
	    'MI', v_Stat_Codes_MI,
	    'ML', v_Stat_Codes_ML,
	    'MP', v_Stat_Codes_MP,
	    'M2', v_Stat_Codes_M2,
	    'NE', v_Stat_Codes_NE,
	    'PC', v_Stat_Codes_PC,
	    'PH', v_Stat_Codes_PH,
	    'PM', v_Stat_Codes_PM,
	    'RB', v_Stat_Codes_RB,
	    'RG', v_Stat_Codes_RG,
	    'RI', v_Stat_Codes_RI,
	    'RL', v_Stat_Codes_RL,
	    'RM', v_Stat_Codes_RM,
	    'RN', v_Stat_Codes_RN,
	    'RP', v_Stat_Codes_RP,
	    'RQ', v_Stat_Codes_RQ,
	    'SM', v_Stat_Codes_SM,
	    'TH', v_Stat_Codes_TH,
	    'VL', v_Stat_Codes_VL,
	    'VP', v_Stat_Codes_VP,
	    'VN', v_Stat_Codes_VN,
	    'VC', v_Stat_Codes_VC,
	    'WC', v_Stat_Codes_WC,
	    'WL', v_Stat_Codes_WL,
	    'CR', v_Stat_Code_CR,
	    'PF', v_Stat_Code_PF,
	    'PI', v_Stat_Code_PI,
	    'PL', v_Stat_Code_PL,
	    'WP', v_Stat_Code_WP,
	    v_statistical_code
	) AS V_Formatted_Stat_Codes,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,1,25)
	SUBSTR(V_Formatted_Stat_Codes, 1, 25) AS Formatted_Stat_Codes,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,26,9)
	SUBSTR(V_Formatted_Stat_Codes, 26, 9) AS Formatted_Stat_Codes_26_34,
	-- *INF*: SUBSTR(V_Formatted_Stat_Codes,35,4)
	SUBSTR(V_Formatted_Stat_Codes, 35, 4) AS Formatted_Stat_Codes_34_38,
	-- *INF*: DECODE(Type_Bureau,'AI', (v_pos_11 || v_pos_12),
	-- 'AL', (v_pos_15  ||  v_pos_16),
	-- 'AN',(v_pos_12 || v_pos_13),
	-- 'AP',(v_pos_12 || v_pos_13),
	-- 'A2',(v_pos_8 || v_pos_9),
	-- 'A3',(v_pos_8 || v_pos_9),
	-- 'BB',(v_pos_20 || v_pos_21),
	-- 'BC',(v_pos_18 || v_pos_19),
	-- 'BE', ( v_pos_4  || v_pos_5),
	-- 'BF', (v_pos_1  ||  v_pos_2),
	-- 'BP', (' '  ||  v_pos_2),
	-- 'BI', (v_pos_3 ||  v_pos_4),
	-- 'BL', (v_pos_3  ||  v_pos_4),
	-- 'BM',(v_pos_20 || v_pos_21),
	-- 'BT', (v_pos_11  ||  v_pos_12),
	-- 'B2',(v_pos_14  ||  v_pos_15),
	-- 'CF', (v_pos_8  || v_pos_9),
	-- 'CI',(v_pos_3  ||  v_pos_4),
	-- 'CN', (v_pos_1  ||  v_pos_2),
	-- 'CP', (v_pos_3  ||  v_pos_4),
	-- 'EI', (v_pos_2  ||  v_pos_3),
	-- 'EQ', (v_pos_8  || v_pos_9),
	-- 'FF', (v_pos_8  || v_pos_9),
	-- 'FI', (v_pos_1  ||  v_pos_2),
	-- 'FM', (v_pos_6  ||  v_pos_7),
	-- 'FO', (v_pos_8  || v_pos_9),
	-- 'FP', (v_pos_2  ||  v_pos_3),
	-- 'FT', (v_pos_4  ||  v_pos_5),
	-- 'GI', (v_pos_10  ||  v_pos_11),
	-- 'GL',(v_pos_20 || v_pos_21),
	-- 'GM', (v_pos_1  ||  v_pos_2),
	-- 'GP', (v_pos_8  || v_pos_9),
	-- 'GS',(v_pos_3  ||  v_pos_4),
	-- 'II', (v_pos_1  ||  v_pos_2),
	-- 'IM', (v_pos_1  ||  v_pos_2),
	-- 'MI',(v_pos_10  ||  v_pos_11),
	-- 'ML', (v_pos_16  ||  v_pos_17),
	-- 'MP', (v_pos_1  ||  v_pos_2),
	-- 'M2', (v_pos_15  ||  v_pos_16),'  ')
	-- 
	-- 
	-- 
	-- 
	DECODE(
	    Type_Bureau,
	    'AI', (v_pos_11 || v_pos_12),
	    'AL', (v_pos_15 || v_pos_16),
	    'AN', (v_pos_12 || v_pos_13),
	    'AP', (v_pos_12 || v_pos_13),
	    'A2', (v_pos_8 || v_pos_9),
	    'A3', (v_pos_8 || v_pos_9),
	    'BB', (v_pos_20 || v_pos_21),
	    'BC', (v_pos_18 || v_pos_19),
	    'BE', (v_pos_4 || v_pos_5),
	    'BF', (v_pos_1 || v_pos_2),
	    'BP', (' ' || v_pos_2),
	    'BI', (v_pos_3 || v_pos_4),
	    'BL', (v_pos_3 || v_pos_4),
	    'BM', (v_pos_20 || v_pos_21),
	    'BT', (v_pos_11 || v_pos_12),
	    'B2', (v_pos_14 || v_pos_15),
	    'CF', (v_pos_8 || v_pos_9),
	    'CI', (v_pos_3 || v_pos_4),
	    'CN', (v_pos_1 || v_pos_2),
	    'CP', (v_pos_3 || v_pos_4),
	    'EI', (v_pos_2 || v_pos_3),
	    'EQ', (v_pos_8 || v_pos_9),
	    'FF', (v_pos_8 || v_pos_9),
	    'FI', (v_pos_1 || v_pos_2),
	    'FM', (v_pos_6 || v_pos_7),
	    'FO', (v_pos_8 || v_pos_9),
	    'FP', (v_pos_2 || v_pos_3),
	    'FT', (v_pos_4 || v_pos_5),
	    'GI', (v_pos_10 || v_pos_11),
	    'GL', (v_pos_20 || v_pos_21),
	    'GM', (v_pos_1 || v_pos_2),
	    'GP', (v_pos_8 || v_pos_9),
	    'GS', (v_pos_3 || v_pos_4),
	    'II', (v_pos_1 || v_pos_2),
	    'IM', (v_pos_1 || v_pos_2),
	    'MI', (v_pos_10 || v_pos_11),
	    'ML', (v_pos_16 || v_pos_17),
	    'MP', (v_pos_1 || v_pos_2),
	    'M2', (v_pos_15 || v_pos_16),
	    '  '
	) AS V_Policy_Type,
	V_Policy_Type AS Policy_Type,
	-- *INF*: SUBSTR(sar_class_code,1,3)
	SUBSTR(sar_class_code, 1, 3) AS v_sar_class_3,
	-- *INF*: DECODE(TRUE,
	-- IN (Type_Bureau,'BP','FP','BF','FT'),V_Policy_Type)
	DECODE(
	    TRUE,
	    Type_Bureau IN ('BP','FP','BF','FT'), V_Policy_Type
	) AS v_type_policy_45,
	-- *INF*: DECODE(TRUE,
	-- Type_Bureau='BP',v_pos_2,
	-- Type_Bureau='BF',v_pos_2,
	-- Type_Bureau='FP',' ',
	-- Type_Bureau='FT',' '  )
	DECODE(
	    TRUE,
	    Type_Bureau = 'BP', v_pos_2,
	    Type_Bureau = 'BF', v_pos_2,
	    Type_Bureau = 'FP', ' ',
	    Type_Bureau = 'FT', ' '
	) AS v_type_of_bond_6,
	-- *INF*: DECODE(TRUE,
	--  IN(Type_Bureau,'BP','BF','FP','FT'),v_sar_class_3  || v_type_policy_45 || v_type_of_bond_6,
	-- sar_class_code)
	DECODE(
	    TRUE,
	    Type_Bureau IN ('BP','BF','FP','FT'), v_sar_class_3 || v_type_policy_45 || v_type_of_bond_6,
	    sar_class_code
	) AS v_hold_sar_class_code,
	v_hold_sar_class_code AS sar_class_code_out
	FROM JNR_Reinsurance_Coverage_n_Transaction
),
LKP_gtamTM08_stage AS (
	SELECT
	coverage_code,
	major_peril
	FROM (
		SELECT gtam_tm08_stage.coverage_code as coverage_code, 
		RTRIM(gtam_tm08_stage.major_peril) as major_peril 
		FROM gtam_tm08_stage
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY major_peril ORDER BY coverage_code DESC) = 1
),
EXP_Generate_Part_7_Reinsurance AS (
	SELECT
	JNR_Reinsurance_Coverage_n_Transaction.reins_ins_line AS reins_insurance_line,
	-- *INF*: IIF(ISNULL(reins_insurance_line) OR LENGTH(reins_insurance_line)=0,'NA',reins_insurance_line)
	IFF(
	    reins_insurance_line IS NULL OR LENGTH(reins_insurance_line) = 0, 'NA', reins_insurance_line
	) AS v_reins_insurance_line,
	JNR_Reinsurance_Coverage_n_Transaction.reins_loc_unit_num AS reins_location_number,
	-- *INF*: IIF(ISNULL(reins_location_number),'0000',LPAD(LTRIM(RTRIM(TO_CHAR(reins_location_number))),4,'0'))
	-- 
	IFF(
	    reins_location_number IS NULL, '0000',
	    LPAD(LTRIM(RTRIM(TO_CHAR(reins_location_number))), 4, '0')
	) AS v_reins_location_number,
	JNR_Reinsurance_Coverage_n_Transaction.reins_sub_loc_unit_num AS reins_sub_location_number,
	-- *INF*: IIF(ISNULL(reins_sub_location_number),'000',LPAD(LTRIM(RTRIM(TO_CHAR(reins_sub_location_number))),3,'0'))
	IFF(
	    reins_sub_location_number IS NULL, '000',
	    LPAD(LTRIM(RTRIM(TO_CHAR(reins_sub_location_number))), 3, '0')
	) AS v_reins_sub_location_number,
	JNR_Reinsurance_Coverage_n_Transaction.reins_risk_unit_grp AS reins_risk_unit_group,
	-- *INF*: IIF(ISNULL(reins_risk_unit_group),'000',TO_CHAR(reins_risk_unit_group))
	IFF(reins_risk_unit_group IS NULL, '000', TO_CHAR(reins_risk_unit_group)) AS v_reins_risk_unit_group,
	JNR_Reinsurance_Coverage_n_Transaction.reins_risk_unit_grp_seq_num AS reins_seq_rsk_unt_grp,
	-- *INF*: IIF(ISNULL(reins_seq_rsk_unt_grp),'000',LPAD(TO_CHAR(reins_seq_rsk_unt_grp),3,'0'))
	IFF(reins_seq_rsk_unt_grp IS NULL, '000', LPAD(TO_CHAR(reins_seq_rsk_unt_grp), 3, '0')) AS v_reins_seq_rsk_unt_grp,
	JNR_Reinsurance_Coverage_n_Transaction.reins_risk_unit AS reins_location,
	v_reins_insurance_line || '-'  || v_reins_location_number  || '-' || v_reins_sub_location_number  || ' - '  ||  v_reins_risk_unit_group  || '-'  ||  v_reins_seq_rsk_unt_grp  || '-'  || reins_location  ||  '000' AS Total_Reins_Key,
	JNR_Reinsurance_Coverage_n_Transaction.reins_risk_unit_seq_num AS reins_risk_sequence,
	JNR_Reinsurance_Coverage_n_Transaction.reins_section_code,
	JNR_Reinsurance_Coverage_n_Transaction.reins_co_num AS reins_company_no,
	JNR_Reinsurance_Coverage_n_Transaction.reins_eff_date AS reins_effective_date,
	JNR_Reinsurance_Coverage_n_Transaction.reins_exp_date AS reins_expiration_date,
	JNR_Reinsurance_Coverage_n_Transaction.reins_type,
	JNR_Reinsurance_Coverage_n_Transaction.reins_prcnt_loss_ceded,
	JNR_Reinsurance_Coverage_n_Transaction.reins_prcnt_facultative_commssn AS reins_percent_fac_comm,
	JNR_Reinsurance_Coverage_n_Transaction.risk_unit_complete AS loss_unit,
	JNR_Reinsurance_Coverage_n_Transaction.ins_line_out AS sar_insurance_line,
	JNR_Reinsurance_Coverage_n_Transaction.loc_unit_num_out AS sar_location_x,
	JNR_Reinsurance_Coverage_n_Transaction.sub_loc_unit_num_out AS sar_sub_location_x,
	JNR_Reinsurance_Coverage_n_Transaction.risk_unit_grp_out AS sar_risk_unit_group,
	JNR_Reinsurance_Coverage_n_Transaction.risk_unit AS sar_unit,
	JNR_Reinsurance_Coverage_n_Transaction.risk_unit_seq_num_out AS sar_seq_rsk_unt_a,
	-- *INF*: TO_DECIMAL(sar_location_x)
	CAST(sar_location_x AS FLOAT) AS v_sar_location,
	JNR_Reinsurance_Coverage_n_Transaction.PMS_Account_Date_To_Check,
	JNR_Reinsurance_Coverage_n_Transaction.major_peril_code_Out AS loss_major_peril,
	JNR_Reinsurance_Coverage_n_Transaction.pms_trans_code,
	JNR_Reinsurance_Coverage_n_Transaction.rsn_amended_code_Out AS Reason_Amended,
	JNR_Reinsurance_Coverage_n_Transaction.section_code_Out AS sar_section,
	JNR_Reinsurance_Coverage_n_Transaction.trans_amt,
	JNR_Reinsurance_Coverage_n_Transaction.trans_hist_amt,
	JNR_Reinsurance_Coverage_n_Transaction.LM_Amount_OutStanding,
	JNR_Reinsurance_Coverage_n_Transaction.LM_Amount_Paid_Losses,
	JNR_Reinsurance_Coverage_n_Transaction.LM_Amount_Paid_Expenses,
	JNR_Reinsurance_Coverage_n_Transaction.LM_Unpaid_Loss_Adj_Exp,
	-- *INF*: LM_Amount_OutStanding  *  reins_prcnt_loss_ceded
	-- ---IIF(IN(pms_trans_code,'90','92','95','97','98','99') AND pms_trans_code <> '95',
	-- ----(trans_amt * reins_prcnt_loss_ceded),0.0)
	LM_Amount_OutStanding * reins_prcnt_loss_ceded AS V_LM_Amount_Outstanding,
	V_LM_Amount_Outstanding AS LM_Reins_Amount_Outstanding,
	-- *INF*:  LM_Amount_Paid_Losses  *  reins_prcnt_loss_ceded
	-- 
	-- 
	-- ---IIF(NOT IN(pms_trans_code,'90','92','95','97','98','99') 
	--     ---   AND NOT IN(pms_trans_code,'71','72','73','74','75','76','77','78',   '79'),
	--        ----(trans_amt* reins_prcnt_loss_ceded),0.0)
	-- 
	-- 
	-- ---IIF(NOT IN(pms_trans_code,'71','72','73','74','75','76','77','78','79'),
	-- ----(loss_paid_or_resv_amt* reins_prcnt_loss_ceded),0.0)
	LM_Amount_Paid_Losses * reins_prcnt_loss_ceded AS V_LM_Amount_Paid_Losses,
	-- *INF*: V_LM_Amount_Paid_Losses
	-- 
	-- --- LM_Amount_Paid_Losses  *  reins_prcnt_loss_ceded
	-- 
	-- --IIF(IN(pms_trans_code,'82','84','89'), 
	-- --- -1 * V_LM_Amount_Paid_Losses, V_LM_Amount_Paid_Losses)
	-- 
	-- 
	V_LM_Amount_Paid_Losses AS LM_Reins_Amount_Paid_Losses,
	-- *INF*: LM_Amount_Paid_Expenses * reins_prcnt_loss_ceded 
	-- 
	-- --IIF(IN(pms_trans_code,'71','72','73','74','75','76','77','78','79'),
	-- ----(trans_amt*reins_prcnt_loss_ceded),0.0)
	LM_Amount_Paid_Expenses * reins_prcnt_loss_ceded AS V_LM_Amount_Paid_Expenses,
	-- *INF*: V_LM_Amount_Paid_Expenses
	-- 
	-- ---IIF(pms_trans_code = '75' ,-1 * V_LM_Amount_Paid_Expenses,
	-- ---V_LM_Amount_Paid_Expenses)
	-- 
	-- ---- R21EX,R22EX,R23EX,R24EX converts into 75 when it backfeeds along with amount field being multiplied with -ve sign.
	-- 
	-- 
	-- 
	-- --
	V_LM_Amount_Paid_Expenses AS LM_Reins_Amount_Paid_Expenses,
	-- *INF*: LM_Unpaid_Loss_Adj_Exp  *  reins_prcnt_loss_ceded 
	-- 
	-- 
	-- ---IIF(IN(pms_trans_code,'90','92','95','97','98','99') AND pms_trans_code = '95',
	-- ---(trans_amt * reins_prcnt_loss_ceded),0.0)
	LM_Unpaid_Loss_Adj_Exp * reins_prcnt_loss_ceded AS V_LM_EOM_Unpaid_Loss_Adj_Exp,
	V_LM_EOM_Unpaid_Loss_Adj_Exp AS LM_Reins_EOM_Unpaid_Loss_Adj_Exp,
	-- *INF*: --IIF(IN(pif_line_business,'CPP','BO','BOP','SMP','GL','CA','CF','CG','CR','IM','BM'),'MORECHECK','NOCHECK')
	'' AS Check_Type,
	-- *INF*: IIF((loss_date<=reins_expiration_date) AND (reins_effective_date <=loss_date) AND 
	-- reins_prcnt_loss_ceded <> 0.0 
	--          ,'VALID','INVALID')
	-- 
	-- 
	IFF(
	    (loss_date <= reins_expiration_date)
	    and (reins_effective_date <= loss_date)
	    and reins_prcnt_loss_ceded <> 0.0,
	    'VALID',
	    'INVALID'
	) AS v_reins_validity,
	-- *INF*: DECODE(v_reins_validity,'INVALID','NOREINSURANCE',
	--                                       'VALID',
	-- DECODE(TRUE,
	-- v_reins_insurance_line = 'GL' and v_reins_location_number <> sar_location_x,'NOREINSURANCE',
	-- reins_section_code =  sar_section AND reins_location=loss_unit_3pos_out OR reins_location = 'N/A', 'REINSURANCE',
	-- (sar_section = '2' AND IN(Reason_Amended,'COL','CWO')) AND  (reins_location = 'N/A'  OR reins_location=loss_unit_3pos_out),
	-- 'REINSURANCE')
	-- )
	-- 
	-- ----- 05/12/2011 Changed the below logic to above logic to insurance_line = 'GL' and based on location_num
	-- 
	-- --DECODE(v_reins_validity,'INVALID','NOREINSURANCE',
	-- --                                      'VALID',
	-- --IIF(reins_section_code =  sar_section AND reins_location=loss_unit_3pos_out OR IS_SPACES(reins_location), 'REINSURANCE',
	-- --IIF((sar_section = '2' AND IN(Reason_Amended,'COL','CWO')) AND
	-- -- (IS_SPACES(reins_location) OR reins_location=loss_unit_3pos_out),
	-- --'REINSURANCE','NOREINSURANCE'))
	-- --)
	-- 
	-- --- Changed the logic 
	-- 
	-- --DECODE(v_reins_validity,'INVALID','NOREINSURANCE',
	-- --'VALID',
	-- --IIF(ISNULL(reins_location_number),
	-- --IIF(reins_location=loss_unit OR IS_SPACES(reins_location) OR reins_location = '0','REINSURANCE','NOREINSURANCE'),
	-- --IIF(
	-- --(sar_section = '2' AND IN(Reason_Amended,'COL','CWO')) AND (IS_SPACES(reins_location) OR reins_location=loss_unit)
	-- --,
	-- --'REINSURANCE','NOREINSURANCE'))
	-- --)
	DECODE(
	    v_reins_validity,
	    'INVALID', 'NOREINSURANCE',
	    'VALID', DECODE(
	        TRUE,
	        v_reins_insurance_line = 'GL' and v_reins_location_number <> sar_location_x, 'NOREINSURANCE',
	        reins_section_code = sar_section AND reins_location = loss_unit_3pos_out OR reins_location = 'N/A', 'REINSURANCE',
	        (sar_section = '2' AND Reason_Amended IN ('COL','CWO')) AND (reins_location = 'N/A' OR reins_location = loss_unit_3pos_out), 'REINSURANCE'
	    )
	) AS v_reinsurance_flag,
	v_reinsurance_flag AS reinsurance_flag,
	-- *INF*: DECODE(v_reinsurance_flag,'NOREINSURANCE','NOGENERATE',
	--                                                              'REINSURANCE',
	--                  IIF(reins_company_no = '0058' and IN(pms_trans_code ,'73','75','76','77'),'NOGENERATE','GENERATE'))
	-- 
	-- --- Added the Reinsurance_Company No logic on 11/11/2010
	-- ----IIF(v_INSURANCE_LINE_GL = 'VALID','GENERATE','NOGENERATE')
	DECODE(
	    v_reinsurance_flag,
	    'NOREINSURANCE', 'NOGENERATE',
	    'REINSURANCE', IFF(
	        reins_company_no = '0058' and pms_trans_code IN ('73','75','76','77'), 'NOGENERATE',
	        'GENERATE'
	    )
	) AS v_Reins_generate_flag,
	v_Reins_generate_flag AS V_Generate_flag,
	-- *INF*: IIF(v_Reins_generate_flag = 'GENERATE' ,
	-- IIF(V_LM_Amount_Outstanding = 0.0 AND 
	--        V_LM_Amount_Paid_Losses= 0.0 AND
	-- V_LM_Amount_Paid_Expenses = 0.0 AND
	-- V_LM_EOM_Unpaid_Loss_Adj_Exp = 0.0,'FALSE','TRUE'),'FALSE')
	IFF(
	    v_Reins_generate_flag = 'GENERATE',
	    IFF(
	        V_LM_Amount_Outstanding = 0.0
	        and V_LM_Amount_Paid_Losses = 0.0
	        and V_LM_Amount_Paid_Expenses = 0.0
	        and V_LM_EOM_Unpaid_Loss_Adj_Exp = 0.0,
	        'FALSE',
	        'TRUE'
	    ),
	    'FALSE'
	) AS Generate_flag,
	@{pipeline().parameters.ACCOUNT_DATE} AS Account_Entered_Date,
	JNR_Reinsurance_Coverage_n_Transaction.sar_id AS loss_id,
	JNR_Reinsurance_Coverage_n_Transaction.Claim_loss_date AS loss_date,
	JNR_Reinsurance_Coverage_n_Transaction.risk_unit_3pos AS loss_unit_3pos_out,
	sar_insurance_line AS sar_insurance_line_calculated,
	-- *INF*: IIF(sar_insurance_line='GL' and sar_location_x  != '0000','0000',sar_location_x)
	IFF(sar_insurance_line = 'GL' and sar_location_x != '0000', '0000', sar_location_x) AS sar_location_calculated,
	-- *INF*: DECODE(TRUE,
	-- IN(sar_insurance_line,'GL','LI'),'000',
	-- sar_insurance_line != 'GA' OR (sar_insurance_line = 'GA' AND loss_id = '51'),sar_sub_location_x) 
	DECODE(
	    TRUE,
	    sar_insurance_line IN ('GL','LI'), '000',
	    sar_insurance_line != 'GA' OR (sar_insurance_line = 'GA' AND loss_id = '51'), sar_sub_location_x
	) AS sar_sub_location_calculated,
	-- *INF*: --IIF(sar_insurance_line = 'CR','000',sar_risk_unit_group)
	'' AS sar_risk_unit_group_calculated,
	-- *INF*: ---IIF(IN(sar_insurance_line,'CR','CF'),'000',sar_class_code_grp_x || sar_class_code_mem_x)
	'' AS sar_risk_unit_group_seq_calculated,
	-- *INF*: ---IIF(sar_insurance_line = 'GA',sar_unit  || '000',sar_unit || sar_risk_unit_continued)
	'' AS sar_unit_calculated,
	-- *INF*: --sar_insurance_line_calculated || '-'  || sar_location_calculated  || '-' || sar_sub_location_calculated  || ' - '  ||  sar_risk_unit_group_calculated  || '-'  ||  sar_risk_unit_group_seq_calculated  || '-'  || sar_unit_calculated
	'' AS Total_Sar_Key,
	-- *INF*: --IIF(Total_Reins_Key  = Total_Sar_Key , 'TRUE','FALSE')
	'' AS Key_Check,
	JNR_Reinsurance_Coverage_n_Transaction.Policy_Symbol,
	JNR_Reinsurance_Coverage_n_Transaction.Policy_Number,
	JNR_Reinsurance_Coverage_n_Transaction.Policy_Module,
	EXP_Transform_Statistical_Codes.Type_Bureau,
	EXP_Transform_Statistical_Codes.Formatted_Stat_Codes,
	EXP_Transform_Statistical_Codes.Formatted_Stat_Codes_26_34,
	EXP_Transform_Statistical_Codes.Formatted_Stat_Codes_34_38,
	EXP_Transform_Statistical_Codes.Policy_Type,
	-- *INF*: IIF(ISNULL(Policy_Type),'N/A',Policy_Type)
	IFF(Policy_Type IS NULL, 'N/A', Policy_Type) AS Policy_Type_out,
	JNR_Reinsurance_Coverage_n_Transaction.risk_state_prov_code_Out,
	JNR_Reinsurance_Coverage_n_Transaction.risk_zip_code_Out,
	JNR_Reinsurance_Coverage_n_Transaction.terr_code_Out,
	JNR_Reinsurance_Coverage_n_Transaction.tax_loc_Out,
	EXP_Transform_Statistical_Codes.sar_class_code_out AS Class_Code,
	JNR_Reinsurance_Coverage_n_Transaction.exposure_Out,
	JNR_Reinsurance_Coverage_n_Transaction.sub_line_code_Out,
	JNR_Reinsurance_Coverage_n_Transaction.source_sar_asl_Out,
	JNR_Reinsurance_Coverage_n_Transaction.source_sar_prdct_line_Out,
	JNR_Reinsurance_Coverage_n_Transaction.source_sar_sp_use_code,
	JNR_Reinsurance_Coverage_n_Transaction.reins_cov_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.claim_rep_ak_id_H,
	JNR_Reinsurance_Coverage_n_Transaction.claim_rep_ak_id_E,
	JNR_Reinsurance_Coverage_n_Transaction.claim_trans_id,
	JNR_Reinsurance_Coverage_n_Transaction.claim_trans_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.agency_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.contract_cust_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.pol_ak_id_Out1,
	JNR_Reinsurance_Coverage_n_Transaction.claim_occurrence_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.claim_party_occurrence_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.claim_party_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.claim_case_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.claimant_cov_det_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.wc_claimant_det_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.claim_pay_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.cov_ak_id_Out,
	JNR_Reinsurance_Coverage_n_Transaction.temp_pol_trans_ak_id,
	JNR_Reinsurance_Coverage_n_Transaction.new_claim_count,
	JNR_Reinsurance_Coverage_n_Transaction.Statistical_Line AS statistical_line,
	JNR_Reinsurance_Coverage_n_Transaction.inception_Date,
	-- *INF*: SUBSTR(source_sar_prdct_line_Out,1,2)
	SUBSTR(source_sar_prdct_line_Out, 1, 2) AS source_stat_brkdown_line,
	'N/A' AS Default_NA,
	-1 AS Default_ID,
	LKP_gtamTM08_stage.coverage_code,
	-- *INF*: IIF(ISNULL(coverage_code),'N/A',coverage_code)
	IFF(coverage_code IS NULL, 'N/A', coverage_code) AS coverage_code_out,
	JNR_Reinsurance_Coverage_n_Transaction.Loss_Master_Run_Date AS loss_master_run_date,
	JNR_Reinsurance_Coverage_n_Transaction.LM_Variation_Code,
	JNR_Reinsurance_Coverage_n_Transaction.trans_date,
	JNR_Reinsurance_Coverage_n_Transaction.pms_acct_entered_date,
	JNR_Reinsurance_Coverage_n_Transaction.trans_offset_onset_ind,
	JNR_Reinsurance_Coverage_n_Transaction.StatisticalCoverageAKID,
	JNR_Reinsurance_Coverage_n_Transaction.RatingCoverageAKID,
	JNR_Reinsurance_Coverage_n_Transaction.financial_type_code,
	JNR_Reinsurance_Coverage_n_Transaction.trans_code,
	JNR_Reinsurance_Coverage_n_Transaction.PolicyCoverageAKID,
	-- *INF*: IIF(ISNULL(PolicyCoverageAKID),-1,PolicyCoverageAKID)
	IFF(PolicyCoverageAKID IS NULL, - 1, PolicyCoverageAKID) AS PolicyCoverageAKID_Out,
	JNR_Reinsurance_Coverage_n_Transaction.RiskLocationAKID,
	-- *INF*: IIF(ISNULL(RiskLocationAKID),-1,RiskLocationAKID)
	IFF(RiskLocationAKID IS NULL, - 1, RiskLocationAKID) AS RiskLocationAKID_Out,
	JNR_Reinsurance_Coverage_n_Transaction.PremiumTransactionAKID,
	-- *INF*: IIF(ISNULL(PremiumTransactionAKID),-1,PremiumTransactionAKID)
	IFF(PremiumTransactionAKID IS NULL, - 1, PremiumTransactionAKID) AS PremiumTransactionAKID_Out,
	JNR_Reinsurance_Coverage_n_Transaction.BureauStatisticalCodeAKID,
	-- *INF*: IIF(ISNULL(BureauStatisticalCodeAKID),-1,BureauStatisticalCodeAKID)
	IFF(BureauStatisticalCodeAKID IS NULL, - 1, BureauStatisticalCodeAKID) AS BureauStatisticalCodeAKID_Out
	FROM EXP_Transform_Statistical_Codes
	 -- Manually join with JNR_Reinsurance_Coverage_n_Transaction
	LEFT JOIN LKP_gtamTM08_stage
	ON LKP_gtamTM08_stage.major_peril = JNR_Reinsurance_Coverage_n_Transaction.major_peril_code_Out
),
FIL_Reinsurance_Rows AS (
	SELECT
	Policy_Symbol AS pif_symbol, 
	Policy_Number AS pif_policy_number, 
	Policy_Module AS pif_module, 
	pms_trans_code, 
	Generate_flag, 
	Account_Entered_Date, 
	claim_trans_id AS claim_trans_pk_id, 
	claim_trans_ak_id, 
	Default_ID AS claim_reins_trans_pk_id, 
	Default_ID AS claim_reins_trans_ak_id, 
	agency_ak_id, 
	contract_cust_ak_id, 
	pol_ak_id_Out1 AS pol_ak_id, 
	claim_occurrence_ak_id, 
	claim_rep_ak_id_H AS claim_primary_rep_ak_id, 
	claim_rep_ak_id_E AS claim_examiner_ak_id, 
	claim_party_occurrence_ak_id, 
	claim_party_ak_id, 
	claim_case_ak_id, 
	claimant_cov_det_ak_id, 
	wc_claimant_det_ak_id, 
	claim_pay_ak_id, 
	cov_ak_id_Out AS cov_ak_id, 
	temp_pol_trans_ak_id, 
	reins_cov_ak_id, 
	LM_Variation_Code AS variation_code, 
	Policy_Type_out AS pol_type, 
	inception_Date AS incptn_date, 
	loss_master_run_date, 
	new_claim_count, 
	trans_amt, 
	trans_hist_amt, 
	LM_Reins_Amount_Outstanding AS outstanding_amt, 
	LM_Reins_Amount_Paid_Losses AS paid_loss_amt, 
	LM_Reins_Amount_Paid_Expenses AS paid_exp_amt, 
	LM_Reins_EOM_Unpaid_Loss_Adj_Exp AS eom_unpaid_loss_adjust_exp, 
	trans_hist_amt AS orig_reserve, 
	Default_NA AS auto_reins_facility, 
	source_stat_brkdown_line AS statistical_brkdwn_line, 
	Formatted_Stat_Codes AS statistical_code1, 
	Formatted_Stat_Codes_26_34 AS statistical_code2, 
	Formatted_Stat_Codes_34_38 AS statistical_code3, 
	statistical_line, 
	coverage_code_out AS loss_master_cov_code, 
	risk_state_prov_code_Out AS risk_state_prov_code, 
	risk_zip_code_Out AS risk_zip_code, 
	terr_code_Out AS terr_code, 
	tax_loc_Out AS tax_loc, 
	Class_Code AS class_code, 
	exposure_Out AS exposure, 
	sub_line_code_Out AS sub_line_code, 
	source_sar_asl_Out AS source_sar_asl, 
	source_sar_prdct_line_Out AS source_sar_prdct_line, 
	source_sar_sp_use_code, 
	trans_date, 
	pms_acct_entered_date, 
	trans_offset_onset_ind, 
	StatisticalCoverageAKID, 
	RatingCoverageAKID, 
	financial_type_code, 
	trans_code, 
	PolicyCoverageAKID_Out AS PolicyCoverageAKID, 
	RiskLocationAKID_Out AS RiskLocationAKID, 
	PremiumTransactionAKID_Out AS PremiumTransactionAKID, 
	BureauStatisticalCodeAKID_Out AS BureauStatisticalCodeAKID
	FROM EXP_Generate_Part_7_Reinsurance
	WHERE Generate_flag='TRUE'
),
EXP_Determine_AK_ID AS (
	SELECT
	'1' AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('01/01/1800 00:00:01','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('01/01/1800 00:00:01', 'MM/DD/YYYY HH24:MI:SS') AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	SEQ_Loss_Master_Calculation_AK_ID.NEXTVAL AS loss_master_calculation_ak_id,
	claim_trans_pk_id,
	claim_trans_ak_id,
	claim_reins_trans_pk_id,
	claim_reins_trans_ak_id,
	agency_ak_id,
	contract_cust_ak_id,
	pol_ak_id,
	claim_occurrence_ak_id,
	claim_primary_rep_ak_id,
	claim_examiner_ak_id,
	claim_party_occurrence_ak_id,
	claim_party_ak_id,
	claim_case_ak_id,
	claimant_cov_det_ak_id,
	wc_claimant_det_ak_id,
	claim_pay_ak_id,
	cov_ak_id,
	temp_pol_trans_ak_id,
	reins_cov_ak_id,
	'C' AS trans_kind_code,
	variation_code,
	pol_type,
	incptn_date,
	loss_master_run_date,
	new_claim_count,
	trans_amt,
	trans_hist_amt,
	outstanding_amt,
	paid_loss_amt,
	paid_exp_amt,
	eom_unpaid_loss_adjust_exp,
	orig_reserve,
	auto_reins_facility,
	statistical_brkdwn_line,
	statistical_code1,
	-- *INF*: IIF(ISNULL(statistical_code1),'N/A',statistical_code1)
	IFF(statistical_code1 IS NULL, 'N/A', statistical_code1) AS statistical_code1_Out,
	statistical_code2,
	-- *INF*: IIF(ISNULL(statistical_code2),'N/A',statistical_code2)
	IFF(statistical_code2 IS NULL, 'N/A', statistical_code2) AS statistical_code2_Out,
	statistical_code3,
	-- *INF*: IIF(ISNULL(statistical_code3),'N/A',statistical_code3)
	IFF(statistical_code3 IS NULL, 'N/A', statistical_code3) AS statistical_code3_Out,
	statistical_line,
	loss_master_cov_code,
	risk_state_prov_code,
	risk_zip_code,
	terr_code,
	tax_loc,
	class_code,
	exposure,
	sub_line_code,
	source_sar_asl,
	source_sar_prdct_line,
	source_sar_sp_use_code,
	pms_trans_code,
	trans_date,
	pms_acct_entered_date,
	trans_offset_onset_ind,
	StatisticalCoverageAKID,
	-1 AS o_Default,
	RatingCoverageAKID,
	financial_type_code,
	trans_code,
	PolicyCoverageAKID,
	RiskLocationAKID,
	PremiumTransactionAKID,
	BureauStatisticalCodeAKID
	FROM FIL_Reinsurance_Rows
),
loss_master_calculation AS (
	INSERT INTO loss_master_calculation
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, loss_master_calculation_ak_id, claim_trans_pk_id, claim_trans_ak_id, claim_reins_trans_pk_id, claim_reins_trans_ak_id, agency_ak_id, contract_cust_ak_id, pol_ak_id, claim_occurrence_ak_id, claim_primary_rep_ak_id, claim_examiner_ak_id, claim_party_occurrence_ak_id, claim_party_ak_id, claim_case_ak_id, claimant_cov_det_ak_id, wc_claimant_det_ak_id, claim_pay_ak_id, cov_ak_id, temp_pol_trans_ak_id, reins_cov_ak_id, trans_kind_code, variation_code, pol_type, incptn_date, loss_master_run_date, new_claim_count, outstanding_amt, paid_loss_amt, paid_exp_amt, eom_unpaid_loss_adjust_exp, orig_reserve, auto_reins_facility, statistical_brkdwn_line, statistical_code1, statistical_code2, statistical_code3, statistical_line, loss_master_cov_code, risk_state_prov_code, risk_zip_code, terr_code, tax_loc, class_code, exposure, sub_line_code, source_sar_asl, source_sar_prdct_line, source_sar_sp_use_code, pms_trans_code, trans_date, pms_acct_entered_date, trans_offset_onset_ind, claim_trans_amt, claim_trans_hist_amt, RiskLocationAKID, PolicyCoverageAKID, StatisticalCoverageAKID, PremiumTransactionAKID, BureauStatisticalCodeAKID, RatingCoverageAKId, FinancialTypeCode, TransactionCode)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	LOSS_MASTER_CALCULATION_AK_ID, 
	CLAIM_TRANS_PK_ID, 
	CLAIM_TRANS_AK_ID, 
	CLAIM_REINS_TRANS_PK_ID, 
	CLAIM_REINS_TRANS_AK_ID, 
	AGENCY_AK_ID, 
	CONTRACT_CUST_AK_ID, 
	POL_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
	CLAIM_PRIMARY_REP_AK_ID, 
	CLAIM_EXAMINER_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	CLAIM_PARTY_AK_ID, 
	CLAIM_CASE_AK_ID, 
	CLAIMANT_COV_DET_AK_ID, 
	WC_CLAIMANT_DET_AK_ID, 
	CLAIM_PAY_AK_ID, 
	COV_AK_ID, 
	TEMP_POL_TRANS_AK_ID, 
	REINS_COV_AK_ID, 
	TRANS_KIND_CODE, 
	VARIATION_CODE, 
	POL_TYPE, 
	INCPTN_DATE, 
	LOSS_MASTER_RUN_DATE, 
	NEW_CLAIM_COUNT, 
	OUTSTANDING_AMT, 
	PAID_LOSS_AMT, 
	PAID_EXP_AMT, 
	EOM_UNPAID_LOSS_ADJUST_EXP, 
	ORIG_RESERVE, 
	AUTO_REINS_FACILITY, 
	STATISTICAL_BRKDWN_LINE, 
	statistical_code1_Out AS STATISTICAL_CODE1, 
	statistical_code2_Out AS STATISTICAL_CODE2, 
	statistical_code3_Out AS STATISTICAL_CODE3, 
	STATISTICAL_LINE, 
	LOSS_MASTER_COV_CODE, 
	RISK_STATE_PROV_CODE, 
	RISK_ZIP_CODE, 
	TERR_CODE, 
	TAX_LOC, 
	CLASS_CODE, 
	EXPOSURE, 
	SUB_LINE_CODE, 
	SOURCE_SAR_ASL, 
	SOURCE_SAR_PRDCT_LINE, 
	SOURCE_SAR_SP_USE_CODE, 
	PMS_TRANS_CODE, 
	TRANS_DATE, 
	PMS_ACCT_ENTERED_DATE, 
	TRANS_OFFSET_ONSET_IND, 
	trans_amt AS CLAIM_TRANS_AMT, 
	trans_hist_amt AS CLAIM_TRANS_HIST_AMT, 
	RISKLOCATIONAKID, 
	POLICYCOVERAGEAKID, 
	STATISTICALCOVERAGEAKID, 
	PREMIUMTRANSACTIONAKID, 
	BUREAUSTATISTICALCODEAKID, 
	RatingCoverageAKID AS RATINGCOVERAGEAKID, 
	financial_type_code AS FINANCIALTYPECODE, 
	trans_code AS TRANSACTIONCODE
	FROM EXP_Determine_AK_ID
),