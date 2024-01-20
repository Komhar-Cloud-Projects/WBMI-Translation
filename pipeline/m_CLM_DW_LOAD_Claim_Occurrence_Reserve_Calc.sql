WITH
SQ_claim_party_occurrence AS (
	SELECT   claim_party_occurrence.claim_party_occurrence_ak_id, claim_party_occurrence.claim_occurrence_ak_id 
	FROM  claim_party_occurrence
	WHERE claim_party_occurrence.crrnt_snpsht_flag = 1
	ORDER BY claim_party_occurrence.claim_occurrence_ak_id
),
SQ_claimant_reserve_calculation AS (
	SELECT a.claim_party_occurrence_ak_id, a.financial_type_code, a.reserve_date, a.reserve_date_type, a.source_sys_id 
	           FROM  dbo.claimant_reserve_calculation a
	WHERE a.claim_party_occurrence_ak_id IN
	(SELECT claim_party_occurrence_ak_id FROM claim_party_occurrence where claim_occurrence_ak_id IN (
	(SELECT  d.claim_occurrence_ak_id FROM claimant_reserve_calculation c
	INNER JOIN Claim_Party_Occurrence d on c.claim_party_occurrence_ak_id = d.claim_party_occurrence_ak_id
	                            WHERE  c.created_date >= '@{pipeline().parameters.SELECTION_START_TS}')))
	
	
	----Changed the Code to fix the calc table issue -- Uma 7/10/2009
	 
	---SELECT a.claim_party_occurrence_ak_id, a.financial_type_code, a.reserve_date, a.reserve_date_type, a.source_sys_id 
	---FROM  dbo.claimant_reserve_calculation a
	---WHERE a.claim_party_occurrence_ak_id IN
	    ----                      (SELECT     c.claim_party_occurrence_ak_id
	         ---                   FROM          claimant_reserve_calculation c
	             ---               WHERE      c.created_date >= '@{pipeline().parameters.SELECTION_START_TS}')
),
JNR_Claimant_Reserve AS (SELECT
	SQ_claim_party_occurrence.claim_party_occurrence_ak_id, 
	SQ_claim_party_occurrence.claim_occurrence_ak_id, 
	SQ_claimant_reserve_calculation.claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id1, 
	SQ_claimant_reserve_calculation.financial_type_code, 
	SQ_claimant_reserve_calculation.reserve_date, 
	SQ_claimant_reserve_calculation.reserve_date_type, 
	SQ_claimant_reserve_calculation.source_sys_id
	FROM SQ_claimant_reserve_calculation
	INNER JOIN SQ_claim_party_occurrence
	ON SQ_claim_party_occurrence.claim_party_occurrence_ak_id = SQ_claimant_reserve_calculation.claim_party_occurrence_ak_id
),
SRT_Claimant_Reserve AS (
	SELECT
	claim_occurrence_ak_id, 
	reserve_date, 
	financial_type_code, 
	reserve_date_type, 
	claim_party_occurrence_ak_id1 AS claim_party_occurrence_ak_id, 
	source_sys_id
	FROM JNR_Claimant_Reserve
	ORDER BY claim_occurrence_ak_id ASC, reserve_date ASC, financial_type_code ASC, reserve_date_type ASC
),
EXP_default AS (
	SELECT
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id AS claim_party_occurrence_ak_id1,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	source_sys_id
	FROM SRT_Claimant_Reserve
),
LKP_work_claim_occurrence_reserve_calculation AS (
	SELECT
	NewLookupRow,
	work_claim_occurrence_reserve_calculation_id,
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	source_sys_id,
	IN_claim_occurrence_ak_id,
	IN_claim_party_occurrence_ak_id,
	IN_financial_type_code,
	IN_reserve_date,
	IN_reserve_date_type,
	IN_source_sys_id
	FROM (
		SELECT 
			NewLookupRow,
			work_claim_occurrence_reserve_calculation_id,
			claim_occurrence_ak_id,
			claim_party_occurrence_ak_id,
			financial_type_code,
			reserve_date,
			reserve_date_type,
			source_sys_id,
			IN_claim_occurrence_ak_id,
			IN_claim_party_occurrence_ak_id,
			IN_financial_type_code,
			IN_reserve_date,
			IN_reserve_date_type,
			IN_source_sys_id
		FROM work_claim_occurrence_reserve_calculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,claim_party_occurrence_ak_id,financial_type_code ORDER BY NewLookupRow) = 1
),
EXP_Detect_Insert_Update AS (
	SELECT
	NewLookupRow,
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	source_sys_id,
	-- *INF*: IIF(claim_occurrence_ak_id=v_prev_row_claim_occurrence_ak_id,'OLD','NEW')
	IFF(claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id, 'OLD', 'NEW') AS v_Claim,
	-- *INF*: IIF(v_Claim ='NEW', 'TRUNCATE' ,DECODE(NewLookupRow,1,'INSERT',
	--                                                        	                                                                     2,'UPDATE',
	-- 	                                                                                                                        0,'NOCHANGE'))
	-- 
	-- 
	-- 
	-- 
	IFF(
	    v_Claim = 'NEW', 'TRUNCATE',
	    DECODE(
	        NewLookupRow,
	        1, 'INSERT',
	        2, 'UPDATE',
	        0, 'NOCHANGE'
	    )
	) AS v_Out_SQL,
	v_Out_SQL AS OUT_SQL,
	claim_occurrence_ak_id AS v_prev_row_claim_occurrence_ak_id
	FROM LKP_work_claim_occurrence_reserve_calculation
),
FIL_INSERT_UPDATE AS (
	SELECT
	claim_occurrence_ak_id, 
	claim_party_occurrence_ak_id, 
	financial_type_code, 
	reserve_date, 
	reserve_date_type, 
	source_sys_id, 
	OUT_SQL
	FROM EXP_Detect_Insert_Update
	WHERE OUT_SQL != 'NOCHANGE'
),
proc_work_claim_occurrence_reserve_calculation AS (
),
EXP_Insert_Noninserts AS (
	SELECT
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	source_sys_id,
	count_claimants,
	count_open_reopen,
	count_notice_only,
	-- *INF*: IIF(claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id, IIF(financial_type_code = 'D' AND v_claim_D_Old = 'NEW' or v_claim_D_Old = 'OLD', 'OLD', 'NEW' ), IIF(financial_type_code = 'D', 'NEW', 'NA'))
	IFF(
	    claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'D'
	    and v_claim_D_Old = 'NEW'
	    or v_claim_D_Old = 'OLD', 'OLD',
	        'NEW'
	    ),
	    IFF(
	        financial_type_code = 'D', 'NEW', 'NA'
	    )
	) AS v_claim_D,
	v_claim_D AS v_claim_D_Old,
	-- *INF*: IIF(claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id, IIF(financial_type_code = 'E' AND v_claim_E_Old = 'NEW' or v_claim_E_Old = 'OLD', 'OLD', 'NEW' ), IIF(financial_type_code = 'E', 'NEW', 'NA'))
	IFF(
	    claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'E'
	    and v_claim_E_Old = 'NEW'
	    or v_claim_E_Old = 'OLD', 'OLD',
	        'NEW'
	    ),
	    IFF(
	        financial_type_code = 'E', 'NEW', 'NA'
	    )
	) AS v_claim_E,
	v_claim_E AS v_claim_E_Old,
	-- *INF*: IIF(claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id, IIF(financial_type_code = 'S' AND v_claim_S_Old = 'NEW' or v_claim_S_Old = 'OLD', 'OLD', 'NEW' ), IIF(financial_type_code = 'S', 'NEW', 'NA'))
	IFF(
	    claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'S'
	    and v_claim_S_Old = 'NEW'
	    or v_claim_S_Old = 'OLD', 'OLD',
	        'NEW'
	    ),
	    IFF(
	        financial_type_code = 'S', 'NEW', 'NA'
	    )
	) AS v_claim_S,
	v_claim_S AS v_claim_S_Old,
	-- *INF*: IIF(claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id, IIF(financial_type_code = 'B' AND v_claim_B_Old = 'NEW' or v_claim_B_Old = 'OLD', 'OLD', 'NEW' ), IIF(financial_type_code = 'B', 'NEW', 'NA'))
	IFF(
	    claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'B'
	    and v_claim_B_Old = 'NEW'
	    or v_claim_B_Old = 'OLD', 'OLD',
	        'NEW'
	    ),
	    IFF(
	        financial_type_code = 'B', 'NEW', 'NA'
	    )
	) AS v_claim_B,
	v_claim_B AS v_claim_B_Old,
	-- *INF*: IIF(claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id, IIF(financial_type_code = 'R' AND v_claim_R_Old = 'NEW' or v_claim_R_Old = 'OLD', 'OLD', 'NEW' ), IIF(financial_type_code = 'R', 'NEW', 'NA'))
	IFF(
	    claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'R'
	    and v_claim_R_Old = 'NEW'
	    or v_claim_R_Old = 'OLD', 'OLD',
	        'NEW'
	    ),
	    IFF(
	        financial_type_code = 'R', 'NEW', 'NA'
	    )
	) AS v_claim_R,
	v_claim_R AS v_claim_R_Old,
	-- *INF*: IIF(v_claim_D='NEW',IIF(financial_type_code = 'D', reserve_date_type, 'NA'), IIF(financial_type_code = 'D',reserve_date_type, v_claim_fin_type_D_old))
	IFF(
	    v_claim_D = 'NEW',
	    IFF(
	        financial_type_code = 'D', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'D', reserve_date_type, v_claim_fin_type_D_old
	    )
	) AS v_claim_fin_type_D,
	v_claim_fin_type_D AS v_claim_fin_type_D_old,
	-- *INF*: IIF(v_claim_E='NEW',IIF(financial_type_code = 'E', reserve_date_type, 'NA'), IIF(financial_type_code = 'E',reserve_date_type, v_claim_fin_type_E_old))
	IFF(
	    v_claim_E = 'NEW',
	    IFF(
	        financial_type_code = 'E', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'E', reserve_date_type, v_claim_fin_type_E_old
	    )
	) AS v_claim_fin_type_E,
	v_claim_fin_type_E AS v_claim_fin_type_E_old,
	-- *INF*: IIF(v_claim_S='NEW',IIF(financial_type_code = 'S', reserve_date_type, 'NA'), IIF(financial_type_code = 'S',reserve_date_type, v_claim_fin_type_S_old))
	IFF(
	    v_claim_S = 'NEW',
	    IFF(
	        financial_type_code = 'S', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'S', reserve_date_type, v_claim_fin_type_S_old
	    )
	) AS v_claim_fin_type_S,
	v_claim_fin_type_S AS v_claim_fin_type_S_old,
	-- *INF*: IIF(v_claim_B='NEW',IIF(financial_type_code = 'B', reserve_date_type, 'NA'), IIF(financial_type_code = 'B',reserve_date_type, v_claim_fin_type_B_old))
	IFF(
	    v_claim_B = 'NEW',
	    IFF(
	        financial_type_code = 'B', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'B', reserve_date_type, v_claim_fin_type_B_old
	    )
	) AS v_claim_fin_type_B,
	v_claim_fin_type_B AS v_claim_fin_type_B_old,
	-- *INF*: IIF(v_claim_R='NEW',IIF(financial_type_code = 'R', reserve_date_type, 'NA'), IIF(financial_type_code = 'R',reserve_date_type, v_claim_fin_type_R_old))
	IFF(
	    v_claim_R = 'NEW',
	    IFF(
	        financial_type_code = 'R', reserve_date_type, 'NA'
	    ),
	    IFF(
	        financial_type_code = 'R', reserve_date_type, v_claim_fin_type_R_old
	    )
	) AS v_claim_fin_type_R,
	v_claim_fin_type_R AS v_claim_fin_type_R_old,
	-- *INF*: IIF(financial_type_code='D',IIF(count_claimants = count_notice_only,'1NOTICEONLY',IIF(count_open_reopen>=1,'2OPEN','3CLOSED')),IIF(v_claim_D = 'OLD' OR v_claim_D = 'NEW',v_claim_overall_status_old_D))
	IFF(
	    financial_type_code = 'D',
	    IFF(
	        count_claimants = count_notice_only, '1NOTICEONLY',
	        IFF(
	            count_open_reopen >= 1, '2OPEN', '3CLOSED'
	        )
	    ),
	    IFF(
	        v_claim_D = 'OLD' OR v_claim_D = 'NEW', v_claim_overall_status_old_D
	    )
	) AS v_claim_overall_status_crrnt_D,
	-- *INF*: IIF(financial_type_code='E',IIF(count_claimants = count_notice_only,'1NOTICEONLY',IIF(count_open_reopen>=1,'2OPEN','3CLOSED')),IIF(v_claim_E = 'OLD' OR v_claim_E = 'NEW',v_claim_overall_status_old_E))
	IFF(
	    financial_type_code = 'E',
	    IFF(
	        count_claimants = count_notice_only, '1NOTICEONLY',
	        IFF(
	            count_open_reopen >= 1, '2OPEN', '3CLOSED'
	        )
	    ),
	    IFF(
	        v_claim_E = 'OLD' OR v_claim_E = 'NEW', v_claim_overall_status_old_E
	    )
	) AS v_claim_overall_status_crrnt_E,
	-- *INF*: IIF(financial_type_code='S',IIF(count_claimants = count_notice_only,'1NOTICEONLY',IIF(count_open_reopen>=1,'2OPEN','3CLOSED')),IIF(v_claim_S = 'OLD' OR v_claim_S = 'NEW',v_claim_overall_status_old_S))
	IFF(
	    financial_type_code = 'S',
	    IFF(
	        count_claimants = count_notice_only, '1NOTICEONLY',
	        IFF(
	            count_open_reopen >= 1, '2OPEN', '3CLOSED'
	        )
	    ),
	    IFF(
	        v_claim_S = 'OLD' OR v_claim_S = 'NEW', v_claim_overall_status_old_S
	    )
	) AS v_claim_overall_status_crrnt_S,
	-- *INF*: IIF(financial_type_code='B',IIF(count_claimants = count_notice_only,'1NOTICEONLY',IIF(count_open_reopen>=1,'2OPEN','3CLOSED')),IIF(v_claim_B = 'OLD' OR v_claim_B = 'NEW',v_claim_overall_status_old_B))
	IFF(
	    financial_type_code = 'B',
	    IFF(
	        count_claimants = count_notice_only, '1NOTICEONLY',
	        IFF(
	            count_open_reopen >= 1, '2OPEN', '3CLOSED'
	        )
	    ),
	    IFF(
	        v_claim_B = 'OLD' OR v_claim_B = 'NEW', v_claim_overall_status_old_B
	    )
	) AS v_claim_overall_status_crrnt_B,
	-- *INF*: IIF(financial_type_code='R',IIF(count_claimants = count_notice_only,'1NOTICEONLY',IIF(count_open_reopen>=1,'2OPEN','3CLOSED')),IIF(v_claim_R = 'OLD' OR v_claim_R = 'NEW',v_claim_overall_status_old_R))
	IFF(
	    financial_type_code = 'R',
	    IFF(
	        count_claimants = count_notice_only, '1NOTICEONLY',
	        IFF(
	            count_open_reopen >= 1, '2OPEN', '3CLOSED'
	        )
	    ),
	    IFF(
	        v_claim_R = 'OLD' OR v_claim_R = 'NEW', v_claim_overall_status_old_R
	    )
	) AS v_claim_overall_status_crrnt_R,
	-- *INF*: IIF(claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id, 
	-- 
	-- IIF(financial_type_code = 'D', IIF(v_claim_D = 'NEW', 'INSERT', IIF(v_claim_overall_status_crrnt_D = v_claim_overall_status_old_D, 'NOCHANGE', 'INSERT')), 
	-- IIF(financial_type_code = 'E', IIF(v_claim_E = 'NEW', 'INSERT', IIF(v_claim_overall_status_crrnt_E = v_claim_overall_status_old_E, 'NOCHANGE', 'INSERT')), 
	-- IIF(financial_type_code = 'S', IIF(v_claim_S = 'NEW', 'INSERT', IIF(v_claim_overall_status_crrnt_S = v_claim_overall_status_old_S, 'NOCHANGE', 'INSERT')),
	-- IIF(financial_type_code = 'B', IIF(v_claim_B = 'NEW', 'INSERT', IIF(v_claim_overall_status_crrnt_B = v_claim_overall_status_old_B, 'NOCHANGE', 'INSERT')), 
	-- IIF(financial_type_code = 'R', IIF(v_claim_R = 'NEW', 'INSERT', IIF(v_claim_overall_status_crrnt_R = v_claim_overall_status_old_R, 'NOCHANGE', 'INSERT'))
	-- ))))),'INSERT')
	IFF(
	    claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id,
	    IFF(
	        financial_type_code = 'D',
	        IFF(
	            v_claim_D = 'NEW', 'INSERT',
	            IFF(
	                v_claim_overall_status_crrnt_D = v_claim_overall_status_old_D,
	                'NOCHANGE',
	                'INSERT'
	            )
	        ),
	        IFF(
	            financial_type_code = 'E',
	            IFF(
	                v_claim_E = 'NEW', 'INSERT',
	                IFF(
	                    v_claim_overall_status_crrnt_E = v_claim_overall_status_old_E,
	                    'NOCHANGE',
	                    'INSERT'
	                )
	            ),
	            IFF(
	                financial_type_code = 'S',
	                IFF(
	                    v_claim_S = 'NEW', 'INSERT',
	                    IFF(
	                        v_claim_overall_status_crrnt_S = v_claim_overall_status_old_S,
	                        'NOCHANGE',
	                        'INSERT'
	                    )
	                ),
	                IFF(
	                    financial_type_code = 'B',
	                    IFF(
	                        v_claim_B = 'NEW', 'INSERT',
	                        IFF(
	                            v_claim_overall_status_crrnt_B = v_claim_overall_status_old_B,
	                            'NOCHANGE',
	                            'INSERT'
	                        )
	                    ),
	                    IFF(
	                        financial_type_code = 'R',
	                        IFF(
	                            v_claim_R = 'NEW', 'INSERT',
	                            IFF(
	                                v_claim_overall_status_crrnt_R = v_claim_overall_status_old_R,
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
	-- DECODE(v_claim_overall_status_crrnt_D,'1NOTICEONLY','1NOTICEONLY',
	--                                                                                      '2OPEN', IIF(v_claim_overall_status_old_D = '3CLOSED',IIF(v_claim_D = 'OLD','4REOPEN', '2OPEN'), '2OPEN')
	--                                                                                     ,'3CLOSED',IIF(v_claim_reserve_date_type_out_old_D ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED')) ,v_claim_reserve_date_type_out_old_D),v_claim_reserve_date_type_out_old_D)
	-- 
	-- 
	-- 
	-- --IIF(financial_type_code = 'D', IIF(v_claim_overall_status_crrnt_D = '2OPEN',IIF(v_claim_overall_status_old_D = '3CLOSED',
	--  --IIF(v_claim_D = 'OLD','4REOPEN', '2OPEN'), '2OPEN'),IIF(v_claim_overall_status_crrnt_D ='3CLOSED',IIF(v_claim_reserve_date_type_out_old_D ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED'),v_claim_overall_status_crrnt_D)),IIF(v_claim_D = 'OLD',v_claim_reserve_date_type_out_old_D)) 
	-- 
	IFF(
	    financial_type_code = 'D',
	    IFF(
	        v_insert_flag = 'INSERT',
	        DECODE(
	            v_claim_overall_status_crrnt_D,
	            '1NOTICEONLY', '1NOTICEONLY',
	            '2OPEN', IFF(
	                v_claim_overall_status_old_D = '3CLOSED',
	                IFF(
	                    v_claim_D = 'OLD', '4REOPEN', '2OPEN'
	                ),
	                '2OPEN'
	            ),
	            '3CLOSED', IFF(
	                v_claim_reserve_date_type_out_old_D = '4REOPEN',
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        ),
	        v_claim_reserve_date_type_out_old_D
	    ),
	    v_claim_reserve_date_type_out_old_D
	) AS v_claim_reserve_date_type_out_crrnt_D,
	-- *INF*: IIF(financial_type_code = 'E',
	-- IIF(v_insert_flag = 'INSERT',
	-- DECODE(v_claim_overall_status_crrnt_E,'1NOTICEONLY','1NOTICEONLY',
	--                                                                                      '2OPEN', IIF(v_claim_overall_status_old_E = '3CLOSED',IIF(v_claim_E = 'OLD','4REOPEN', '2OPEN'), '2OPEN')
	--                                                                                     ,'3CLOSED',IIF(v_claim_reserve_date_type_out_old_E ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED')) ,v_claim_reserve_date_type_out_old_E),v_claim_reserve_date_type_out_old_E)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --IIF(financial_type_code = 'E', IIF(v_claim_overall_status_crrnt_E = '2OPEN',IIF(v_claim_overall_status_old_E = '3CLOSED',
	-- --IIF(v_claim_E = 'OLD','4REOPEN', '2OPEN'), '2OPEN'),IIF(v_claim_overall_status_crrnt_E ='3CLOSED',IIF(v_claim_reserve_date_type_out_old_E ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED'),v_claim_overall_status_crrnt_E)),IIF(v_claim_E = 'OLD',v_claim_reserve_date_type_out_old_E)) 
	IFF(
	    financial_type_code = 'E',
	    IFF(
	        v_insert_flag = 'INSERT',
	        DECODE(
	            v_claim_overall_status_crrnt_E,
	            '1NOTICEONLY', '1NOTICEONLY',
	            '2OPEN', IFF(
	                v_claim_overall_status_old_E = '3CLOSED',
	                IFF(
	                    v_claim_E = 'OLD', '4REOPEN', '2OPEN'
	                ),
	                '2OPEN'
	            ),
	            '3CLOSED', IFF(
	                v_claim_reserve_date_type_out_old_E = '4REOPEN',
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        ),
	        v_claim_reserve_date_type_out_old_E
	    ),
	    v_claim_reserve_date_type_out_old_E
	) AS v_claim_reserve_date_type_out_crrnt_E,
	-- *INF*: IIF(financial_type_code = 'S',
	-- IIF(v_insert_flag = 'INSERT',
	-- DECODE(v_claim_overall_status_crrnt_S,'1NOTICEONLY','1NOTICEONLY',
	--                                                                                      '2OPEN', IIF(v_claim_overall_status_old_S = '3CLOSED',IIF(v_claim_S = 'OLD','4REOPEN', '2OPEN'), '2OPEN')
	--                                                                                     ,'3CLOSED',IIF(v_claim_reserve_date_type_out_old_S ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED')) ,v_claim_reserve_date_type_out_old_S),v_claim_reserve_date_type_out_old_S)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --IIF(financial_type_code = 'S', IIF(v_claim_overall_status_crrnt_S = '2OPEN',IIF(v_claim_overall_status_old_S = '3CLOSED',
	-- -- IIF(v_claim_S = 'OLD','4REOPEN', '2OPEN'), '2OPEN'),IIF(v_claim_overall_status_crrnt_S ='3CLOSED',IIF(v_claim_reserve_date_type_out_old_S ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED'),v_claim_overall_status_crrnt_S)),IIF(v_claim_S = 'OLD',v_claim_reserve_date_type_out_old_S)) 
	IFF(
	    financial_type_code = 'S',
	    IFF(
	        v_insert_flag = 'INSERT',
	        DECODE(
	            v_claim_overall_status_crrnt_S,
	            '1NOTICEONLY', '1NOTICEONLY',
	            '2OPEN', IFF(
	                v_claim_overall_status_old_S = '3CLOSED',
	                IFF(
	                    v_claim_S = 'OLD', '4REOPEN', '2OPEN'
	                ),
	                '2OPEN'
	            ),
	            '3CLOSED', IFF(
	                v_claim_reserve_date_type_out_old_S = '4REOPEN',
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        ),
	        v_claim_reserve_date_type_out_old_S
	    ),
	    v_claim_reserve_date_type_out_old_S
	) AS v_claim_reserve_date_type_out_crrnt_S,
	-- *INF*: IIF(financial_type_code = 'B',
	-- IIF(v_insert_flag = 'INSERT',
	-- DECODE(v_claim_overall_status_crrnt_B,'1NOTICEONLY','1NOTICEONLY',
	--                                                                                      '2OPEN', IIF(v_claim_overall_status_old_B = '3CLOSED',IIF(v_claim_B = 'OLD','4REOPEN', '2OPEN'), '2OPEN')
	--                                                                                     ,'3CLOSED',IIF(v_claim_reserve_date_type_out_old_B ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED')) ,v_claim_reserve_date_type_out_old_B),v_claim_reserve_date_type_out_old_B)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --IIF(financial_type_code = 'B', IIF(v_claim_overall_status_crrnt_B = '2OPEN',IIF(v_claim_overall_status_old_B = '3CLOSED',
	-- -- IIF(v_claim_B = 'OLD','4REOPEN', '2OPEN'), '2OPEN'),IIF(v_claim_overall_status_crrnt_B ='3CLOSED',IIF(v_claim_reserve_date_type_out_old_B ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED'),v_claim_overall_status_crrnt_B)),IIF(v_claim_B = 'OLD',v_claim_reserve_date_type_out_old_B)) 
	IFF(
	    financial_type_code = 'B',
	    IFF(
	        v_insert_flag = 'INSERT',
	        DECODE(
	            v_claim_overall_status_crrnt_B,
	            '1NOTICEONLY', '1NOTICEONLY',
	            '2OPEN', IFF(
	                v_claim_overall_status_old_B = '3CLOSED',
	                IFF(
	                    v_claim_B = 'OLD', '4REOPEN', '2OPEN'
	                ),
	                '2OPEN'
	            ),
	            '3CLOSED', IFF(
	                v_claim_reserve_date_type_out_old_B = '4REOPEN',
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        ),
	        v_claim_reserve_date_type_out_old_B
	    ),
	    v_claim_reserve_date_type_out_old_B
	) AS v_claim_reserve_date_type_out_crrnt_B,
	-- *INF*: IIF(financial_type_code = 'R',
	-- IIF(v_insert_flag = 'INSERT',
	-- DECODE(v_claim_overall_status_crrnt_R,'1NOTICEONLY','1NOTICEONLY',
	--                                                                                      '2OPEN', IIF(v_claim_overall_status_old_R = '3CLOSED',IIF(v_claim_R = 'OLD','4REOPEN', '2OPEN'), '2OPEN')
	--                                                                                     ,'3CLOSED',IIF(v_claim_reserve_date_type_out_old_R ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED')) ,v_claim_reserve_date_type_out_old_R),v_claim_reserve_date_type_out_old_R)
	-- 
	-- 
	-- 
	-- --IIF(financial_type_code = 'R', IIF(v_claim_overall_status_crrnt_R = '2OPEN',IIF(v_claim_overall_status_old_R = '3CLOSED',
	-- --IIF(v_claim_R = 'OLD','4REOPEN', '2OPEN'), '2OPEN'),IIF(v_claim_overall_status_crrnt_R ='3CLOSED',IIF(v_claim_reserve_date_type_out_old_R ='4REOPEN','5CLOSEDAFTERREOPEN','3CLOSED'),v_claim_overall_status_crrnt_R)),IIF(v_claim_R = 'OLD',v_claim_reserve_date_type_out_old_R))
	IFF(
	    financial_type_code = 'R',
	    IFF(
	        v_insert_flag = 'INSERT',
	        DECODE(
	            v_claim_overall_status_crrnt_R,
	            '1NOTICEONLY', '1NOTICEONLY',
	            '2OPEN', IFF(
	                v_claim_overall_status_old_R = '3CLOSED',
	                IFF(
	                    v_claim_R = 'OLD', '4REOPEN', '2OPEN'
	                ),
	                '2OPEN'
	            ),
	            '3CLOSED', IFF(
	                v_claim_reserve_date_type_out_old_R = '4REOPEN',
	                '5CLOSEDAFTERREOPEN',
	                '3CLOSED'
	            )
	        ),
	        v_claim_reserve_date_type_out_old_R
	    ),
	    v_claim_reserve_date_type_out_old_R
	) AS v_claim_reserve_date_type_out_crrnt_R,
	-- *INF*: DECODE(financial_type_code,'D',v_claim_reserve_date_type_out_crrnt_D,
	--                                                                'E',v_claim_reserve_date_type_out_crrnt_E,
	-- 		                                                  'S',v_claim_reserve_date_type_out_crrnt_S,
	--                                                                'B',v_claim_reserve_date_type_out_crrnt_B,
	--                                                                'R',v_claim_reserve_date_type_out_crrnt_R)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- ---IIF(financial_type_code ='D',v_claim_reserve_date_type_out_crrnt_D,
	-- --IIF(financial_type_code ='E',v_claim_reserve_date_type_out_crrnt_E,
	-- --IIF(financial_type_code ='S',v_claim_reserve_date_type_out_crrnt_S,
	-- --IIF(financial_type_code ='B',v_claim_reserve_date_type_out_crrnt_B,
	-- --IIF(financial_type_code ='R',v_claim_reserve_date_type_out_crrnt_R)))))
	DECODE(
	    financial_type_code,
	    'D', v_claim_reserve_date_type_out_crrnt_D,
	    'E', v_claim_reserve_date_type_out_crrnt_E,
	    'S', v_claim_reserve_date_type_out_crrnt_S,
	    'B', v_claim_reserve_date_type_out_crrnt_B,
	    'R', v_claim_reserve_date_type_out_crrnt_R
	) AS claim_reserve_date_type_out,
	v_claim_reserve_date_type_out_crrnt_D AS v_claim_reserve_date_type_out_old_D,
	v_claim_reserve_date_type_out_crrnt_E AS v_claim_reserve_date_type_out_old_E,
	v_claim_reserve_date_type_out_crrnt_S AS v_claim_reserve_date_type_out_old_S,
	v_claim_reserve_date_type_out_crrnt_B AS v_claim_reserve_date_type_out_old_B,
	v_claim_reserve_date_type_out_crrnt_R AS v_claim_reserve_date_type_out_old_R,
	v_claim_overall_status_crrnt_D AS v_claim_overall_status_old_D,
	v_claim_overall_status_crrnt_E AS v_claim_overall_status_old_E,
	v_claim_overall_status_crrnt_S AS v_claim_overall_status_old_S,
	v_claim_overall_status_crrnt_B AS v_claim_overall_status_old_B,
	v_claim_overall_status_crrnt_R AS v_claim_overall_status_old_R,
	claim_occurrence_ak_id AS v_prev_row_claim_occurrence_ak_id
	FROM proc_work_claim_occurrence_reserve_calculation
),
FIL_INSERTS AS (
	SELECT
	claim_occurrence_ak_id, 
	financial_type_code, 
	reserve_date, 
	claim_reserve_date_type_out AS reserve_date_type, 
	source_sys_id, 
	insert_flag_out
	FROM EXP_Insert_Noninserts
	WHERE insert_flag_out ='INSERT'
),
LKP_Claim_Occurrence_reserve_calculation AS (
	SELECT
	claim_occurrence_reserve_calculation_id,
	claim_occurrence_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	IN_claim_occurrence_ak_id,
	IN_financial_type_code,
	IN_reserve_date,
	IN_reserve_date_type
	FROM (
		SELECT 
			claim_occurrence_reserve_calculation_id,
			claim_occurrence_ak_id,
			financial_type_code,
			reserve_date,
			reserve_date_type,
			IN_claim_occurrence_ak_id,
			IN_financial_type_code,
			IN_reserve_date,
			IN_reserve_date_type
		FROM claim_occurrence_reserve_calculation
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_occurrence_ak_id,financial_type_code,reserve_date,reserve_date_type ORDER BY claim_occurrence_reserve_calculation_id) = 1
),
FIL_CLAIMS AS (
	SELECT
	LKP_Claim_Occurrence_reserve_calculation.claim_occurrence_reserve_calculation_id AS lkp_claim_occurrence_reserve_calculation_id, 
	FIL_INSERTS.claim_occurrence_ak_id, 
	FIL_INSERTS.financial_type_code, 
	FIL_INSERTS.reserve_date, 
	FIL_INSERTS.reserve_date_type, 
	FIL_INSERTS.source_sys_id
	FROM FIL_INSERTS
	LEFT JOIN LKP_Claim_Occurrence_reserve_calculation
	ON LKP_Claim_Occurrence_reserve_calculation.claim_occurrence_ak_id = FIL_INSERTS.claim_occurrence_ak_id AND LKP_Claim_Occurrence_reserve_calculation.financial_type_code = FIL_INSERTS.financial_type_code AND LKP_Claim_Occurrence_reserve_calculation.reserve_date = FIL_INSERTS.reserve_date AND LKP_Claim_Occurrence_reserve_calculation.reserve_date_type = FIL_INSERTS.reserve_date_type
	WHERE IIF(ISNULL(lkp_claim_occurrence_reserve_calculation_id),TRUE,FALSE)
),
EXP_Metadata AS (
	SELECT
	claim_occurrence_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type,
	-- *INF*: DECODE(reserve_date_type,'1NOTICEONLY','N',
	--                                                              '2OPEN','O',
	--                                                              '3CLOSED','C',
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
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM FIL_CLAIMS
),
SEQ_Claim_Occ_Rsrv_Calc_AK_ID AS (
	CREATE SEQUENCE SEQ_Claim_Occ_Rsrv_Calc_AK_ID
	START = 0
	INCREMENT = 1;
),
claim_occurrence_reserve_calculation AS (
	INSERT INTO claim_occurrence_reserve_calculation
	(claim_occurrence_reserve_calculation_ak_id, claim_occurrence_ak_id, financial_type_code, reserve_date, reserve_date_type, financial_type_status_code, crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date)
	SELECT 
	SEQ_Claim_Occ_Rsrv_Calc_AK_ID.NEXTVAL AS CLAIM_OCCURRENCE_RESERVE_CALCULATION_AK_ID, 
	CLAIM_OCCURRENCE_AK_ID, 
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
SQ_claim_occurrence_reserve_calculation_update AS (
	SELECT a.claim_occurrence_reserve_calculation_id, a.claim_occurrence_ak_id, a.financial_type_code, a.reserve_date_type,a.eff_from_date, a.eff_to_date, a.source_sys_id 
	FROM  @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence_reserve_calculation a
	where EXISTS (SELECT 1 
	            FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.claim_occurrence_reserve_calculation b
	WHERE b.crrnt_snpsht_flag = 1
	AND a.claim_occurrence_ak_id = b.claim_occurrence_ak_id 
	AND a.source_sys_id = b.source_sys_id
	AND a.financial_type_code = b.financial_type_code 
	GROUP BY b.claim_occurrence_ak_id, b.financial_type_code, b.source_sys_id
		HAVING COUNT(*) > 1)
	ORDER BY a.claim_occurrence_ak_id, a.financial_type_code, a.source_sys_id,a.eff_from_date  DESC,a.claim_occurrence_reserve_calculation_ak_id DESC
	
	
	--EXISTS Subquery exists to pick AK Groups that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of eff_to_date='12/31/2100 23:59:59' and all columns of the AK
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
	
	--ORDER BY of main query orders all rows first by the AK and then by the eff_from_date in a DESC format
	--the descending order is important because this allows us to avoid another lookup and properly apply the eff_to_date by utilizing a local variable to keep track
	
	-- In the order by clause we added claim_occurrence_reserve_calculation_ak_id  DESC ,because say a claim has staus order of 
	-- '4REOPEN',
	-- '5CLOSEDAFTERREOPEN',
	-- '4REOPEN' on same day for PMS data , then the latest row with '4REOPEN' status should have a crrnt_snpsht_flag value of  1.
),
EXP_Expire_rows AS (
	SELECT
	claim_occurrence_reserve_calculation_id,
	claim_occurrence_ak_id,
	financial_type_code,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	source_sys_id,
	-- *INF*: DECODE(TRUE,claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id AND source_sys_id = v_source_sys_id AND financial_type_code = v_prev_row_financial_type_code, ADD_TO_DATE(v_eff_from_date,'SS',-1),
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
	    claim_occurrence_ak_id = v_prev_row_claim_occurrence_ak_id AND source_sys_id = v_source_sys_id AND financial_type_code = v_prev_row_financial_type_code, DATEADD(SECOND,- 1,v_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	claim_occurrence_ak_id AS v_prev_row_claim_occurrence_ak_id,
	financial_type_code AS v_prev_row_financial_type_code,
	eff_from_date AS v_eff_from_date,
	source_sys_id AS v_source_sys_id,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date,
	reserve_date_type
	FROM SQ_claim_occurrence_reserve_calculation_update
),
FIL_claim_Occcurrence_reserve_calc_UPD AS (
	SELECT
	claim_occurrence_reserve_calculation_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Expire_rows
	WHERE orig_eff_to_date != eff_to_date
),
UPD_Claim_Occurrence_Reserve_Calc AS (
	SELECT
	claim_occurrence_reserve_calculation_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_claim_Occcurrence_reserve_calc_UPD
),
claim_occurrence_reserve_calculation_update AS (
	MERGE INTO claim_occurrence_reserve_calculation AS T
	USING UPD_Claim_Occurrence_Reserve_Calc AS S
	ON T.claim_occurrence_reserve_calculation_id = S.claim_occurrence_reserve_calculation_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),