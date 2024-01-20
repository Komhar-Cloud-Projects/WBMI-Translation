WITH
SQ_claimant_coverage_detail AS (
	SELECT claimant_coverage_detail.claimant_cov_det_ak_id, claimant_coverage_detail.claim_party_occurrence_ak_id 
	FROM claimant_coverage_detail
	WHERE claimant_coverage_detail.crrnt_snpsht_flag = 1  
	ORDER BY claimant_coverage_detail.claim_party_occurrence_ak_id
),
SQ_claimant_coverage_detail_reserve_calculation AS (
	SELECT  a.claimant_cov_det_ak_id, a.financial_type_code, a.reserve_date, a.reserve_date_type, a.source_sys_id
	FROM  dbo.claimant_coverage_detail_reserve_calculation a 
	WHERE a.claimant_cov_det_ak_id IN
	(SELECT claimant_cov_det_ak_id from claimant_coverage_detail where claim_party_occurrence_ak_id in (
	(SELECT  d.claim_party_occurrence_ak_id FROM claimant_coverage_detail_reserve_calculation c
	INNER JOIN claimant_coverage_detail d on c.claimant_cov_det_ak_id=d.claimant_cov_det_ak_id
	                         WHERE      c.created_date >= '@{pipeline().parameters.SELECTION_START_TS}')))
	
	--- 7/6/2009 - For a coverage that has any change, we need to get the corresponding claimant and then we get all the coverages --- for that claimant.
	
	
	--SELECT    a.claimant_cov_det_ak_id, a.financial_type_code, 
	   ---                   a.reserve_date, a.reserve_date_type, a.source_sys_id
	----FROM         dbo.claimant_coverage_detail_reserve_calculation a 
	---WHERE a.claimant_cov_det_ak_id IN
	    ---                  (SELECT     c.claimant_cov_det_ak_id
	        ---                FROM          claimant_coverage_detail_reserve_calculation c
	            -----             WHERE      c.created_date >= '@{pipeline().parameters.SELECTION_START_TS}')
),
JNR_Coverage_Reserve_Coverage_detail AS (SELECT
	SQ_claimant_coverage_detail.claimant_cov_det_ak_id, 
	SQ_claimant_coverage_detail.claim_party_occurrence_ak_id, 
	SQ_claimant_coverage_detail_reserve_calculation.claimant_cov_det_ak_id AS claimant_cov_det_ak_id1, 
	SQ_claimant_coverage_detail_reserve_calculation.financial_type_code, 
	SQ_claimant_coverage_detail_reserve_calculation.reserve_date, 
	SQ_claimant_coverage_detail_reserve_calculation.reserve_date_type, 
	SQ_claimant_coverage_detail_reserve_calculation.source_sys_id
	FROM SQ_claimant_coverage_detail_reserve_calculation
	INNER JOIN SQ_claimant_coverage_detail
	ON SQ_claimant_coverage_detail.claimant_cov_det_ak_id = SQ_claimant_coverage_detail_reserve_calculation.claimant_cov_det_ak_id
),
SRT_Coverage_Reserve AS (
	SELECT
	claim_party_occurrence_ak_id, 
	claimant_cov_det_ak_id1, 
	reserve_date, 
	financial_type_code, 
	reserve_date_type, 
	source_sys_id
	FROM JNR_Coverage_Reserve_Coverage_detail
	ORDER BY claim_party_occurrence_ak_id ASC, reserve_date ASC, financial_type_code ASC, reserve_date_type ASC
),
EXP_default AS (
	SELECT
	claim_party_occurrence_ak_id,
	claimant_cov_det_ak_id1 AS claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	source_sys_id
	FROM SRT_Coverage_Reserve
),
LKP_Work_Claimant_Reserve_Calculation AS (
	SELECT
	NewLookupRow,
	work_claimant_reserve_calculation_id,
	claim_party_occurrence_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	source_sys_id,
	IN_claim_party_occurrence_ak_id,
	IN_claimant_cov_det_ak_id,
	IN_financial_type_code,
	IN_reserve_date,
	IN_reserve_date_type,
	IN_source_sys_id
	FROM (
		SELECT 
			NewLookupRow,
			work_claimant_reserve_calculation_id,
			claim_party_occurrence_ak_id,
			claimant_cov_det_ak_id,
			financial_type_code,
			reserve_date,
			reserve_date_type,
			source_sys_id,
			IN_claim_party_occurrence_ak_id,
			IN_claimant_cov_det_ak_id,
			IN_financial_type_code,
			IN_reserve_date,
			IN_reserve_date_type,
			IN_source_sys_id
		FROM work_claimant_reserve_calculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,claimant_cov_det_ak_id,financial_type_code ORDER BY NewLookupRow) = 1
),
EXP_detect_insert_update AS (
	SELECT
	NewLookupRow,
	claim_party_occurrence_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	source_sys_id,
	-- *INF*: IIF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id ,'OLD','NEW')
	IFF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id, 'OLD', 'NEW') AS v_claimant,
	v_claimant AS v_claimant_out,
	-- *INF*: IIF(v_claimant ='NEW', 'TRUNCATE' ,DECODE(NewLookupRow,1,'INSERT',
	--                                                        	                                                                                   2,'UPDATE',
	-- 	                                                                                                                                0,'NOCHANGE'))
	IFF(
	    v_claimant = 'NEW', 'TRUNCATE',
	    DECODE(
	        NewLookupRow,
	        1, 'INSERT',
	        2, 'UPDATE',
	        0, 'NOCHANGE'
	    )
	) AS V_SQL,
	V_SQL AS OUT_SQL,
	claim_party_occurrence_ak_id AS v_prev_row_claim_party_occurrence_ak_id
	FROM LKP_Work_Claimant_Reserve_Calculation
),
FIL_INSERT_UPDATE AS (
	SELECT
	claim_party_occurrence_ak_id, 
	claimant_cov_det_ak_id, 
	financial_type_code, 
	reserve_date, 
	reserve_date_type, 
	source_sys_id, 
	OUT_SQL
	FROM EXP_detect_insert_update
	WHERE OUT_SQL != 'NOCHANGE'
),
proc_work_claimant_reserve_calculation AS (
),
EXP_Inserts_Noninserts AS (
	SELECT
	claim_party_occurrence_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	source_sys_id,
	count_coverages,
	count_open_reopen,
	count_notice_only,
	-- *INF*: IIF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id, IIF(financial_type_code = 'D' AND v_claimant_D_old = 'NEW' or v_claimant_D_old = 'OLD', 'OLD', 'NEW' ), IIF(financial_type_code = 'D', 'NEW', 'NA'))
	-- 
	IFF(
	    claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'D'
	    and v_claimant_D_old = 'NEW'
	    or v_claimant_D_old = 'OLD',
	        'OLD',
	        'NEW'
	    ),
	    IFF(
	        financial_type_code = 'D', 'NEW', 'NA'
	    )
	) AS v_claimant_D,
	v_claimant_D AS v_claimant_D_old,
	-- *INF*: IIF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id, IIF(financial_type_code = 'E' AND v_claimant_E_old = 'NEW' or v_claimant_E_old = 'OLD', 'OLD' , 'NEW'), IIF(financial_type_code = 'E', 'NEW', 'NA'))
	-- 
	-- 
	-- 
	-- --IIF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id, IIF(financial_type_code = 'D' AND v_claimant_D_old = 'NEW' or v_claimant_D_old = 'OLD', 'OLD', 'NEW' ), IIF(financial_type_code = 'D', 'NEW', 'NA'))
	IFF(
	    claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'E'
	    and v_claimant_E_old = 'NEW'
	    or v_claimant_E_old = 'OLD',
	        'OLD',
	        'NEW'
	    ),
	    IFF(
	        financial_type_code = 'E', 'NEW', 'NA'
	    )
	) AS v_claimant_E,
	v_claimant_E AS v_claimant_E_old,
	-- *INF*: IIF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id, IIF(financial_type_code = 'S' AND v_claimant_S_old = 'NEW' or v_claimant_S_old = 'OLD', 'OLD' , 'NEW'), IIF(financial_type_code = 'S', 'NEW', 'NA'))
	-- 
	-- 
	-- 
	-- --IIF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id, IIF(financial_type_code = 'S' AND v_claimant_S_old = 'NEW' or v_claimant_S_old = 'OLD', 'OLD' ), 'NEW')
	IFF(
	    claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'S'
	    and v_claimant_S_old = 'NEW'
	    or v_claimant_S_old = 'OLD',
	        'OLD',
	        'NEW'
	    ),
	    IFF(
	        financial_type_code = 'S', 'NEW', 'NA'
	    )
	) AS v_claimant_S,
	v_claimant_S AS v_claimant_S_old,
	-- *INF*: IIF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id, IIF(financial_type_code = 'B' AND v_claimant_B_old = 'NEW' or v_claimant_B_old = 'OLD', 'OLD' , 'NEW'), IIF(financial_type_code = 'B', 'NEW', 'NA'))
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --IIF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id, IIF(financial_type_code = 'B' AND v_claimant_B_old = 'NEW' or v_claimant_B_old = 'OLD', 'OLD' ), 'NEW')
	IFF(
	    claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'B'
	    and v_claimant_B_old = 'NEW'
	    or v_claimant_B_old = 'OLD',
	        'OLD',
	        'NEW'
	    ),
	    IFF(
	        financial_type_code = 'B', 'NEW', 'NA'
	    )
	) AS v_claimant_B,
	v_claimant_B AS v_claimant_B_old,
	-- *INF*: IIF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id, IIF(financial_type_code = 'R' AND v_claimant_R_old = 'NEW' or v_claimant_R_old = 'OLD', 'OLD' , 'NEW'), IIF(financial_type_code = 'R', 'NEW', 'NA'))
	-- 
	-- 
	-- 
	-- --IIF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id, IIF(financial_type_code = 'R' AND v_claimant_R_old = 'NEW' or v_claimant_R_old = 'OLD', 'OLD' ), 'NEW')
	IFF(
	    claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'R'
	    and v_claimant_R_old = 'NEW'
	    or v_claimant_R_old = 'OLD',
	        'OLD',
	        'NEW'
	    ),
	    IFF(
	        financial_type_code = 'R', 'NEW', 'NA'
	    )
	) AS v_claimant_R,
	v_claimant_R AS v_claimant_R_old,
	-- *INF*: IIF(v_claimant_D='NEW',IIF(financial_type_code = 'D', reserve_date_type, 'NA'), IIF(financial_type_code = 'D',reserve_date_type, v_claimant_fin_type_D_old))
	-- 
	-- 
	-- --IIF(v_claimant_D='NEW',IIF(financial_type_code = 'D', reserve_date_type, 'NA'), IIF(financial_type_code = 'D',v_claimant_fin_type_D_old))
	IFF(
	    v_claimant_D = 'NEW',
	    IFF(
	        financial_type_code = 'D', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'D', reserve_date_type, v_claimant_fin_type_D_old
	    )
	) AS v_claimant_fin_type_D,
	v_claimant_fin_type_D AS v_claimant_fin_type_D_old,
	-- *INF*: IIF(v_claimant_E='NEW',IIF(financial_type_code='E',reserve_date_type,'NA'),IIF(financial_type_code = 'E', reserve_date_type, v_claimant_fin_type_E_old))
	-- 
	-- 
	-- --IIF(v_claimant='NEW',IIF(financial_type_code='E',reserve_date_type,'NA'),IIF(financial_type_code = 'E', v_claimant_fin_type_E_old))
	IFF(
	    v_claimant_E = 'NEW',
	    IFF(
	        financial_type_code = 'E', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'E', reserve_date_type, v_claimant_fin_type_E_old
	    )
	) AS v_claimant_fin_type_E,
	v_claimant_fin_type_E AS v_claimant_fin_type_E_old,
	-- *INF*: IIF(v_claimant_S='NEW',IIF(financial_type_code='S',reserve_date_type,'NA'),IIF(financial_type_code = 'S', reserve_date_type, v_claimant_fin_type_S_old))
	IFF(
	    v_claimant_S = 'NEW',
	    IFF(
	        financial_type_code = 'S', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'S', reserve_date_type, v_claimant_fin_type_S_old
	    )
	) AS v_claimant_fin_type_S,
	v_claimant_fin_type_S AS v_claimant_fin_type_S_old,
	-- *INF*: IIF(v_claimant_B='NEW',IIF(financial_type_code='B',reserve_date_type,'NA'),IIF(financial_type_code = 'B', reserve_date_type, v_claimant_fin_type_B_old))
	IFF(
	    v_claimant_B = 'NEW',
	    IFF(
	        financial_type_code = 'B', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'B', reserve_date_type, v_claimant_fin_type_B_old
	    )
	) AS v_claimant_fin_type_B,
	v_claimant_fin_type_B AS v_claimant_fin_type_B_old,
	-- *INF*: IIF(v_claimant_R='NEW',IIF(financial_type_code='R',reserve_date_type,'NA'),IIF(financial_type_code = 'R', reserve_date_type, v_claimant_fin_type_R_old))
	IFF(
	    v_claimant_R = 'NEW',
	    IFF(
	        financial_type_code = 'R', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'R', reserve_date_type, v_claimant_fin_type_R_old
	    )
	) AS v_claimant_fin_type_R,
	v_claimant_fin_type_R AS v_claimant_fin_type_R_old,
	-- *INF*: IIF(financial_type_code='D',IIF(count_coverages = count_notice_only,'1NOTICEONLY',IIF(count_open_reopen>=1,'2OPEN','3CLOSED')),IIF(v_claimant_D = 'OLD' OR v_claimant_D = 'NEW'  ,v_claimant_overall_status_old_D))
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    financial_type_code = 'D',
	    IFF(
	        count_coverages = count_notice_only, '1NOTICEONLY',
	        IFF(
	            count_open_reopen >= 1, '2OPEN', '3CLOSED'
	        )
	    ),
	    IFF(
	        v_claimant_D = 'OLD' OR v_claimant_D = 'NEW', v_claimant_overall_status_old_D
	    )
	) AS v_claimant_overall_status_crrnt_D,
	-- *INF*: IIF(financial_type_code='E',IIF(count_coverages = count_notice_only,'1NOTICEONLY',IIF(count_open_reopen>=1,'2OPEN','3CLOSED')),IIF(v_claimant_E = 'OLD' OR v_claimant_E = 'NEW' ,v_claimant_overall_status_old_E))
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    financial_type_code = 'E',
	    IFF(
	        count_coverages = count_notice_only, '1NOTICEONLY',
	        IFF(
	            count_open_reopen >= 1, '2OPEN', '3CLOSED'
	        )
	    ),
	    IFF(
	        v_claimant_E = 'OLD' OR v_claimant_E = 'NEW', v_claimant_overall_status_old_E
	    )
	) AS v_claimant_overall_status_crrnt_E,
	-- *INF*: IIF(financial_type_code='S',IIF(count_coverages = count_notice_only,'1NOTICEONLY',IIF(count_open_reopen>=1,'2OPEN','3CLOSED')),IIF(v_claimant_S = 'OLD' OR v_claimant_S = 'NEW' ,v_claimant_overall_status_old_S))
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    financial_type_code = 'S',
	    IFF(
	        count_coverages = count_notice_only, '1NOTICEONLY',
	        IFF(
	            count_open_reopen >= 1, '2OPEN', '3CLOSED'
	        )
	    ),
	    IFF(
	        v_claimant_S = 'OLD' OR v_claimant_S = 'NEW', v_claimant_overall_status_old_S
	    )
	) AS v_claimant_overall_status_crrnt_S,
	-- *INF*: IIF(financial_type_code='B',IIF(count_coverages = count_notice_only,'1NOTICEONLY',IIF(count_open_reopen>=1,'2OPEN','3CLOSED')),IIF(v_claimant_B = 'OLD' OR v_claimant_B = 'NEW' ,v_claimant_overall_status_old_B))
	-- 
	-- 
	-- 
	-- 
	IFF(
	    financial_type_code = 'B',
	    IFF(
	        count_coverages = count_notice_only, '1NOTICEONLY',
	        IFF(
	            count_open_reopen >= 1, '2OPEN', '3CLOSED'
	        )
	    ),
	    IFF(
	        v_claimant_B = 'OLD' OR v_claimant_B = 'NEW', v_claimant_overall_status_old_B
	    )
	) AS v_claimant_overall_status_crrnt_B,
	-- *INF*: IIF(financial_type_code='R',IIF(count_coverages = count_notice_only,'1NOTICEONLY',IIF(count_open_reopen>=1,'2OPEN','3CLOSED')),IIF(v_claimant_R = 'OLD' OR v_claimant_R = 'NEW' ,v_claimant_overall_status_old_R))
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    financial_type_code = 'R',
	    IFF(
	        count_coverages = count_notice_only, '1NOTICEONLY',
	        IFF(
	            count_open_reopen >= 1, '2OPEN', '3CLOSED'
	        )
	    ),
	    IFF(
	        v_claimant_R = 'OLD' OR v_claimant_R = 'NEW', v_claimant_overall_status_old_R
	    )
	) AS v_claimant_overall_status_crrnt_R,
	-- *INF*: IIF(claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id, 
	-- 
	-- IIF(financial_type_code = 'D', IIF(v_claimant_D = 'NEW', 'INSERT', IIF(v_claimant_overall_status_crrnt_D = v_claimant_overall_status_old_D, 'NOCHANGE', 'INSERT')), 
	-- IIF(financial_type_code = 'E', IIF(v_claimant_E = 'NEW', 'INSERT', IIF(v_claimant_overall_status_crrnt_E = v_claimant_overall_status_old_E, 'NOCHANGE', 'INSERT')), 
	-- IIF(financial_type_code = 'S', IIF(v_claimant_S = 'NEW', 'INSERT', IIF(v_claimant_overall_status_crrnt_S = v_claimant_overall_status_old_S, 'NOCHANGE', 'INSERT')),
	-- IIF(financial_type_code = 'B', IIF(v_claimant_B = 'NEW', 'INSERT', IIF(v_claimant_overall_status_crrnt_B = v_claimant_overall_status_old_B, 'NOCHANGE', 'INSERT')), 
	-- IIF(financial_type_code = 'R', IIF(v_claimant_R = 'NEW', 'INSERT', IIF(v_claimant_overall_status_crrnt_R = v_claimant_overall_status_old_R, 'NOCHANGE', 'INSERT'))
	-- ))))),'INSERT')
	-- 
	-- 
	-- 
	-- 
	IFF(
	    claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'D',
	        IFF(
	            v_claimant_D = 'NEW', 'INSERT',
	            IFF(
	                v_claimant_overall_status_crrnt_D = v_claimant_overall_status_old_D,
	                'NOCHANGE',
	                'INSERT'
	            )
	        ),
	        IFF(
	            financial_type_code = 'E',
	            IFF(
	                v_claimant_E = 'NEW', 'INSERT',
	                IFF(
	                    v_claimant_overall_status_crrnt_E = v_claimant_overall_status_old_E,
	                    'NOCHANGE',
	                    'INSERT'
	                )
	            ),
	            IFF(
	                financial_type_code = 'S',
	                IFF(
	                    v_claimant_S = 'NEW', 'INSERT',
	                    IFF(
	                        v_claimant_overall_status_crrnt_S = v_claimant_overall_status_old_S,
	                        'NOCHANGE',
	                        'INSERT'
	                    )
	                ),
	                IFF(
	                    financial_type_code = 'B',
	                    IFF(
	                        v_claimant_B = 'NEW', 'INSERT',
	                        IFF(
	                            v_claimant_overall_status_crrnt_B = v_claimant_overall_status_old_B,
	                            'NOCHANGE',
	                            'INSERT'
	                        )
	                    ),
	                    IFF(
	                        financial_type_code = 'R',
	                        IFF(
	                            v_claimant_R = 'NEW', 'INSERT',
	                            IFF(
	                                v_claimant_overall_status_crrnt_R = v_claimant_overall_status_old_R,
	                                'NOCHANGE',
	                                'INSERT'
	                            )
	                        )
	                    )
	                )
	            )
	        )
	    ),
	    'INSERT'
	) AS v_insert_flag,
	v_insert_flag AS insert_flag_out,
	-- *INF*: IIF(financial_type_code = 'D',
	-- IIF(v_insert_flag = 'INSERT',
	-- DECODE(v_claimant_overall_status_crrnt_D,'1NOTICEONLY','1NOTICEONLY',
	--                                                                                           '2OPEN', IIF(v_claimant_overall_status_old_D = '3CLOSED',IIF(v_claimant_D = 'OLD','4REOPEN', '2OPEN'), '2OPEN')
	--                                                                                          ,'3CLOSED',IIF(v_claimant_reserve_date_type_out_old_D ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED')) ,v_claimant_reserve_date_type_out_old_D),v_claimant_reserve_date_type_out_old_D)
	-- 
	-- 
	-- 
	-- 
	-- --IIF(financial_type_code = 'D', IIF(v_claimant_overall_status_crrnt_D = '2OPEN',IIF(v_claimant_overall_status_old_D = '3CLOSED',
	--  --IIF(v_claimant_D = 'OLD','4REOPEN', '2OPEN'), '2OPEN'),IIF(v_claimant_overall_status_crrnt_D ='3CLOSED',IIF(v_claimant_reserve_date_type_out_old_D ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED'),v_claimant_overall_status_crrnt_D)),IIF(v_claimant_D = 'OLD',v_claimant_reserve_date_type_out_old_D)) 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    financial_type_code = 'D',
	    IFF(
	        v_insert_flag = 'INSERT',
	        DECODE(
	            v_claimant_overall_status_crrnt_D,
	            '1NOTICEONLY', '1NOTICEONLY',
	            '2OPEN', IFF(
	                v_claimant_overall_status_old_D = '3CLOSED',
	                IFF(
	                    v_claimant_D = 'OLD', '4REOPEN', '2OPEN'
	                ),
	                '2OPEN'
	            ),
	            '3CLOSED', IFF(
	                v_claimant_reserve_date_type_out_old_D = '4REOPEN',
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        ),
	        v_claimant_reserve_date_type_out_old_D
	    ),
	    v_claimant_reserve_date_type_out_old_D
	) AS v_claimant_reserve_date_type_out_crrnt_D,
	-- *INF*: IIF(financial_type_code = 'E',
	-- IIF(v_insert_flag = 'INSERT',
	-- DECODE(v_claimant_overall_status_crrnt_E,'1NOTICEONLY','1NOTICEONLY',
	--                                                                                           '2OPEN', IIF(v_claimant_overall_status_old_E = '3CLOSED',IIF(v_claimant_E = 'OLD','4REOPEN', '2OPEN'), '2OPEN')
	--                                                                                          ,'3CLOSED',IIF(v_claimant_reserve_date_type_out_old_E ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED')) ,v_claimant_reserve_date_type_out_old_E),v_claimant_reserve_date_type_out_old_E)
	-- 
	-- 
	-- 
	-- 
	-- --IIF(financial_type_code = 'E', IIF(v_claimant_overall_status_crrnt_E = '2OPEN',IIF(v_claimant_overall_status_old_E = '3CLOSED',
	-- -- IIF(v_claimant_E = 'OLD','4REOPEN', '2OPEN'), '2OPEN'),IIF(v_claimant_overall_status_crrnt_E ='3CLOSED',IIF(v_claimant_reserve_date_type_out_old_E ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED'),v_claimant_overall_status_crrnt_E)),IIF(v_claimant_E = 'OLD',v_claimant_reserve_date_type_out_old_E)) 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    financial_type_code = 'E',
	    IFF(
	        v_insert_flag = 'INSERT',
	        DECODE(
	            v_claimant_overall_status_crrnt_E,
	            '1NOTICEONLY', '1NOTICEONLY',
	            '2OPEN', IFF(
	                v_claimant_overall_status_old_E = '3CLOSED',
	                IFF(
	                    v_claimant_E = 'OLD', '4REOPEN', '2OPEN'
	                ),
	                '2OPEN'
	            ),
	            '3CLOSED', IFF(
	                v_claimant_reserve_date_type_out_old_E = '4REOPEN',
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        ),
	        v_claimant_reserve_date_type_out_old_E
	    ),
	    v_claimant_reserve_date_type_out_old_E
	) AS v_claimant_reserve_date_type_out_crrnt_E,
	-- *INF*: IIF(financial_type_code = 'S',
	-- IIF(v_insert_flag = 'INSERT',
	-- DECODE(v_claimant_overall_status_crrnt_S,'1NOTICEONLY','1NOTICEONLY',
	--                                                                                           '2OPEN', IIF(v_claimant_overall_status_old_S = '3CLOSED',IIF(v_claimant_S = 'OLD','4REOPEN', '2OPEN'), '2OPEN')
	--                                                                                          ,'3CLOSED',IIF(v_claimant_reserve_date_type_out_old_S ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED')) ,v_claimant_reserve_date_type_out_old_S),v_claimant_reserve_date_type_out_old_S)
	-- 
	-- --------------
	-- 
	-- --IIF(financial_type_code = 'S', IIF(v_claimant_overall_status_crrnt_S = '2OPEN',IIF(v_claimant_overall_status_old_S = '3CLOSED',
	-- -- IIF(v_claimant_S = 'OLD','4REOPEN', '2OPEN'), '2OPEN'),IIF(v_claimant_overall_status_crrnt_S ='3CLOSED',IIF(v_claimant_reserve_date_type_out_old_S ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED'),v_claimant_overall_status_crrnt_S)),IIF(v_claimant_S = 'OLD',v_claimant_reserve_date_type_out_old_S)) 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    financial_type_code = 'S',
	    IFF(
	        v_insert_flag = 'INSERT',
	        DECODE(
	            v_claimant_overall_status_crrnt_S,
	            '1NOTICEONLY', '1NOTICEONLY',
	            '2OPEN', IFF(
	                v_claimant_overall_status_old_S = '3CLOSED',
	                IFF(
	                    v_claimant_S = 'OLD', '4REOPEN', '2OPEN'
	                ),
	                '2OPEN'
	            ),
	            '3CLOSED', IFF(
	                v_claimant_reserve_date_type_out_old_S = '4REOPEN',
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        ),
	        v_claimant_reserve_date_type_out_old_S
	    ),
	    v_claimant_reserve_date_type_out_old_S
	) AS v_claimant_reserve_date_type_out_crrnt_S,
	-- *INF*: IIF(financial_type_code = 'B',
	-- IIF(v_insert_flag = 'INSERT',
	-- DECODE(v_claimant_overall_status_crrnt_B,'1NOTICEONLY','1NOTICEONLY',
	--                                                                                           '2OPEN', IIF(v_claimant_overall_status_old_B = '3CLOSED',IIF(v_claimant_B = 'OLD','4REOPEN', '2OPEN'), '2OPEN')
	--                                                                                          ,'3CLOSED',IIF(v_claimant_reserve_date_type_out_old_B ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED')) ,v_claimant_reserve_date_type_out_old_B),v_claimant_reserve_date_type_out_old_B)
	-- 
	-- ----------------
	-- 
	-- --IIF(financial_type_code = 'B', IIF(v_claimant_overall_status_crrnt_B = '2OPEN',IIF(v_claimant_overall_status_old_B = '3CLOSED',
	-- -- IIF(v_claimant_B = 'OLD','4REOPEN', '2OPEN'), '2OPEN'),IIF(v_claimant_overall_status_crrnt_B ='3CLOSED',IIF(v_claimant_reserve_date_type_out_old_B ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED'),v_claimant_overall_status_crrnt_B)),IIF(v_claimant_B = 'OLD',v_claimant_reserve_date_type_out_old_B)) 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    financial_type_code = 'B',
	    IFF(
	        v_insert_flag = 'INSERT',
	        DECODE(
	            v_claimant_overall_status_crrnt_B,
	            '1NOTICEONLY', '1NOTICEONLY',
	            '2OPEN', IFF(
	                v_claimant_overall_status_old_B = '3CLOSED',
	                IFF(
	                    v_claimant_B = 'OLD', '4REOPEN', '2OPEN'
	                ),
	                '2OPEN'
	            ),
	            '3CLOSED', IFF(
	                v_claimant_reserve_date_type_out_old_B = '4REOPEN',
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        ),
	        v_claimant_reserve_date_type_out_old_B
	    ),
	    v_claimant_reserve_date_type_out_old_B
	) AS v_claimant_reserve_date_type_out_crrnt_B,
	-- *INF*: IIF(financial_type_code = 'R',
	-- IIF(v_insert_flag = 'INSERT',
	-- DECODE(v_claimant_overall_status_crrnt_R,'1NOTICEONLY','1NOTICEONLY',
	--                                                                                           '2OPEN', IIF(v_claimant_overall_status_old_R = '3CLOSED',IIF(v_claimant_R = 'OLD','4REOPEN', '2OPEN'), '2OPEN')
	--                                                                                          ,'3CLOSED',IIF(v_claimant_reserve_date_type_out_old_R ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED')) ,v_claimant_reserve_date_type_out_old_R),v_claimant_reserve_date_type_out_old_R)
	-- 
	-- ----------
	-- 
	-- --IIF(financial_type_code = 'R', IIF(v_claimant_overall_status_crrnt_R = '2OPEN',IIF(v_claimant_overall_status_old_R = '3CLOSED',
	-- -- IIF(v_claimant_R = 'OLD','4REOPEN', '2OPEN'), '2OPEN'),IIF(v_claimant_overall_status_crrnt_R ='3CLOSED',IIF(v_claimant_reserve_date_type_out_old_R ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED'),v_claimant_overall_status_crrnt_R)),IIF(v_claimant_R = 'OLD',v_claimant_reserve_date_type_out_old_R)) 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    financial_type_code = 'R',
	    IFF(
	        v_insert_flag = 'INSERT',
	        DECODE(
	            v_claimant_overall_status_crrnt_R,
	            '1NOTICEONLY', '1NOTICEONLY',
	            '2OPEN', IFF(
	                v_claimant_overall_status_old_R = '3CLOSED',
	                IFF(
	                    v_claimant_R = 'OLD', '4REOPEN', '2OPEN'
	                ),
	                '2OPEN'
	            ),
	            '3CLOSED', IFF(
	                v_claimant_reserve_date_type_out_old_R = '4REOPEN',
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        ),
	        v_claimant_reserve_date_type_out_old_R
	    ),
	    v_claimant_reserve_date_type_out_old_R
	) AS v_claimant_reserve_date_type_out_crrnt_R,
	-- *INF*: DECODE(financial_type_code,'D',v_claimant_reserve_date_type_out_crrnt_D,
	--                                                                'E',v_claimant_reserve_date_type_out_crrnt_E,
	-- 		                                                  'S',v_claimant_reserve_date_type_out_crrnt_S,
	--                                                                'B',v_claimant_reserve_date_type_out_crrnt_B,
	--                                                                'R',v_claimant_reserve_date_type_out_crrnt_R)
	DECODE(
	    financial_type_code,
	    'D', v_claimant_reserve_date_type_out_crrnt_D,
	    'E', v_claimant_reserve_date_type_out_crrnt_E,
	    'S', v_claimant_reserve_date_type_out_crrnt_S,
	    'B', v_claimant_reserve_date_type_out_crrnt_B,
	    'R', v_claimant_reserve_date_type_out_crrnt_R
	) AS claimant_reserve_date_type_out,
	v_claimant_reserve_date_type_out_crrnt_D AS v_claimant_reserve_date_type_out_old_D,
	v_claimant_reserve_date_type_out_crrnt_E AS v_claimant_reserve_date_type_out_old_E,
	v_claimant_reserve_date_type_out_crrnt_S AS v_claimant_reserve_date_type_out_old_S,
	v_claimant_reserve_date_type_out_crrnt_B AS v_claimant_reserve_date_type_out_old_B,
	v_claimant_reserve_date_type_out_crrnt_R AS v_claimant_reserve_date_type_out_old_R,
	v_claimant_overall_status_crrnt_D AS v_claimant_overall_status_old_D,
	v_claimant_overall_status_crrnt_E AS v_claimant_overall_status_old_E,
	v_claimant_overall_status_crrnt_S AS v_claimant_overall_status_old_S,
	v_claimant_overall_status_crrnt_B AS v_claimant_overall_status_old_B,
	v_claimant_overall_status_crrnt_R AS v_claimant_overall_status_old_R,
	claim_party_occurrence_ak_id AS v_prev_row_claim_party_occurrence_ak_id
	FROM proc_work_claimant_reserve_calculation
),
FIL_non_inserts AS (
	SELECT
	insert_flag_out, 
	claim_party_occurrence_ak_id, 
	financial_type_code, 
	reserve_date, 
	claimant_reserve_date_type_out AS reserve_date_type, 
	source_sys_id
	FROM EXP_Inserts_Noninserts
	WHERE insert_flag_out='INSERT'
),
LKP_Claimant_reserve_calculation AS (
	SELECT
	claimant_reserve_calculation_id,
	claim_party_occurrence_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	IN_claim_party_occurrence_ak_id,
	IN_financial_type_code,
	IN_reserve_date,
	IN_reserve_date_type
	FROM (
		SELECT 
			claimant_reserve_calculation_id,
			claim_party_occurrence_ak_id,
			financial_type_code,
			reserve_date,
			reserve_date_type,
			IN_claim_party_occurrence_ak_id,
			IN_financial_type_code,
			IN_reserve_date,
			IN_reserve_date_type
		FROM claimant_reserve_calculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_party_occurrence_ak_id,financial_type_code,reserve_date,reserve_date_type ORDER BY claimant_reserve_calculation_id) = 1
),
FIL_Existing_records AS (
	SELECT
	LKP_Claimant_reserve_calculation.claimant_reserve_calculation_id AS lkp_claimant_reserve_calculation_id, 
	FIL_non_inserts.claim_party_occurrence_ak_id, 
	FIL_non_inserts.financial_type_code, 
	FIL_non_inserts.reserve_date, 
	FIL_non_inserts.reserve_date_type, 
	FIL_non_inserts.source_sys_id
	FROM FIL_non_inserts
	LEFT JOIN LKP_Claimant_reserve_calculation
	ON LKP_Claimant_reserve_calculation.claim_party_occurrence_ak_id = FIL_non_inserts.claim_party_occurrence_ak_id AND LKP_Claimant_reserve_calculation.financial_type_code = FIL_non_inserts.financial_type_code AND LKP_Claimant_reserve_calculation.reserve_date = FIL_non_inserts.reserve_date AND LKP_Claimant_reserve_calculation.reserve_date_type = FIL_non_inserts.reserve_date_type
	WHERE IIF(ISNULL(lkp_claimant_reserve_calculation_id),TRUE,FALSE)
),
EXP_Metadata AS (
	SELECT
	claim_party_occurrence_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	-- *INF*: DECODE(reserve_date_type,'1NOTICEONLY','N',
	--                     						   '2OPEN','O',
	-- 	              					         '3CLOSED','C',
	--                                                              '4REOPEN','O',
	--                                                              '5CLOSEDAFTERREOPEN','C')
	DECODE(
	    reserve_date_type,
	    '1NOTICEONLY', 'N',
	    '2OPEN', 'O',
	    '3CLOSED', 'C',
	    '4REOPEN', 'O',
	    '5CLOSEDAFTERREOPEN', 'C'
	) AS financial_type_status_code,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	reserve_date AS eff_from_date,
	-- *INF*: to_date('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM FIL_Existing_records
),
SEQ_Clmt_Rsrv_Calc_AK_ID AS (
	CREATE SEQUENCE SEQ_Clmt_Rsrv_Calc_AK_ID
	START = 0
	INCREMENT = 1;
),
claimant_reserve_calculation_insert AS (
	INSERT INTO claimant_reserve_calculation
	(claimant_reserve_calculation_ak_id, claim_party_occurrence_ak_id, financial_type_code, reserve_date, reserve_date_type, financial_type_status_code, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Clmt_Rsrv_Calc_AK_ID.NEXTVAL AS CLAIMANT_RESERVE_CALCULATION_AK_ID, 
	CLAIM_PARTY_OCCURRENCE_AK_ID, 
	FINANCIAL_TYPE_CODE, 
	RESERVE_DATE, 
	RESERVE_DATE_TYPE, 
	FINANCIAL_TYPE_STATUS_CODE, 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE
	FROM EXP_Metadata
),
SQ_claimant_reserve_calculation_update AS (
	SELECT a.claimant_reserve_calculation_id, a.claim_party_occurrence_ak_id, a.financial_type_code, a.reserve_date_type,a.eff_from_date, a.eff_to_date, a.source_sys_id 
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_reserve_calculation a
	where EXISTS ( SELECT 1			
		FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claimant_reserve_calculation b
	WHERE b.crrnt_snpsht_flag = 1
	AND a.claim_party_occurrence_ak_id = b.claim_party_occurrence_ak_id 
	AND a.source_sys_id = b.source_sys_id
	AND a.financial_type_code = b.financial_type_code 
	GROUP BY b.claim_party_occurrence_ak_id, b.financial_type_code, b.source_sys_id
		HAVING COUNT(*) > 1)
	ORDER BY a.claim_party_occurrence_ak_id, a.financial_type_code, a.source_sys_id,a.eff_from_date  DESC, a.claimant_reserve_calculation_ak_id DESC
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of eff_to_date='12/31/2100 23:59:59' and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
	
	-- In the order by clause we added claimant_reserve_calculation_ak_id  DESC ,because say a claimant has staus order of 
	-- '4REOPEN',
	-- '5CLOSEDAFTERREOPEN',
	-- '4REOPEN' on same day for PMS data , then the latest row with '4REOPEN' status should have a crrnt_snpsht_flag value of  1.
),
EXP_Expire_rows AS (
	SELECT
	claimant_reserve_calculation_id,
	claim_party_occurrence_ak_id,
	financial_type_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: DECODE(TRUE,claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id AND source_sys_id = v_source_sys_id AND financial_type_code = v_prev_row_financial_type_code, ADD_TO_DATE(v_eff_from_date,'SS',-1),
	-- 	orig_eff_to_date)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    claim_party_occurrence_ak_id = v_prev_row_claim_party_occurrence_ak_id AND source_sys_id = v_source_sys_id AND financial_type_code = v_prev_row_financial_type_code, DATEADD(SECOND,- 1,v_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claim_party_occurrence_ak_id AS v_prev_row_claim_party_occurrence_ak_id,
	financial_type_code AS v_prev_row_financial_type_code,
	eff_from_date AS v_eff_from_date,
	source_sys_id AS v_source_sys_id,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date,
	reserve_date_type
	FROM SQ_claimant_reserve_calculation_update
),
FIL_claimant_reserve_calc_UPD AS (
	SELECT
	claimant_reserve_calculation_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Expire_rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_CLMT_RESERVE_CALC AS (
	SELECT
	claimant_reserve_calculation_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_claimant_reserve_calc_UPD
),
claimant_reserve_calculation_update AS (
	MERGE INTO claimant_reserve_calculation AS T
	USING UPD_CLMT_RESERVE_CALC AS S
	ON T.claimant_reserve_calculation_id = S.claimant_reserve_calculation_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),