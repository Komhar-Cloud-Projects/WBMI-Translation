WITH
LKP_Claim_Suit_status_code AS (
	SELECT
	suit_status_code_descript,
	suit_status_code
	FROM (
		SELECT 
		sup_claim_suit_status_code.suit_status_code_descript as suit_status_code_descript, 
		sup_claim_suit_status_code.suit_status_code as suit_status_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_suit_status_code
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY suit_status_code ORDER BY suit_status_code_descript) = 1
),
LKP_prim_litigation_handler_role_code AS (
	SELECT
	prim_litigation_role_code_descript,
	prim_litigation_role_code
	FROM (
		SELECT 
		sup_claim_primary_litigation_role_code.prim_litigation_role_code_descript as prim_litigation_role_code_descript, sup_claim_primary_litigation_role_code.prim_litigation_role_code as prim_litigation_role_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_claim_primary_litigation_role_code
		WHERE crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY prim_litigation_role_code ORDER BY prim_litigation_role_code_descript) = 1
),
SQ_claim_case AS (
	SELECT DISTINCT cca.claim_case_id,
	                cca.claim_case_ak_id,
	                cca.claim_case_key,
	                cca.claim_case_name,
	                cca.claim_case_num,
	                cca.suit_county,
	                cca.suit_state,
	                cca.trial_date,
	                cca.first_notice_law_suit_ind,
	                cca.declaratory_action_ind,
	                cca.suit_status_code,
	                cca.suit_denial_date,
	                cca.prim_litigation_handler_role_code,
	                cca.suit_open_date,
	                cca.suit_close_date,
	                cca.suit_how_claim_closed,
	                cca.reins_reported_ind,
	                cca.commercl_umb_reserve,
	                cca.suit_pay_amt,
	                cca.arbitration_open_date,
	                cca.arbitration_close_date,
	                distinct_eff_from_dates.eff_from_date AS eff_from_date,
	                cca.demand_at_initial_litigation,
	                prim_lit.claim_party_id               AS prim_lit_pk_id,
	                prim_lit.claim_party_ak_id            AS prim_lit_ak_id,
	                prim_lit.claim_party_full_name        AS prim_lit_full_name,
	                prim_lit.claim_party_first_name       AS prim_lit_first_name,
	                prim_lit.claim_party_last_name        AS prim_lit_last_name,
	                prim_lit.claim_party_mid_name         AS prim_lit_mid_name,
	                prim_lit.claim_party_name_prfx        AS prim_lit_name_prfx,
	                prim_lit.claim_party_name_sfx         AS prim_lit_name_sfx,
	                prim_lit.claim_party_addr             AS prim_lit_addr,
	                prim_lit.claim_party_city             AS prim_lit_city,
	                prim_lit.claim_party_county           AS prim_lit_county,
	                prim_lit.claim_party_state            AS prim_lit_state,
	                prim_lit.claim_party_zip              AS prim_lit_zip,
	                prim_lit.addr_type                    AS prim_adr_type,
	                defd.claim_party_id                   AS def_claim_party_pk_id,
	                defd.claim_party_ak_id                AS def_claim_party_ak_id,
	                defd.claim_party_full_name            AS def_full_name,
	                defd.claim_party_first_name           AS def_first_name,
	                defd.claim_party_last_name            AS def_last_name,
	                defd.claim_party_mid_name             AS def_mid_name,
	                defd.claim_party_name_prfx            AS def_name_prfx,
	                defd.claim_party_name_sfx             AS def_name_sfx,
	                code.claim_party_id                   AS code_claim_party_pk_id,
	                code.claim_party_ak_id                AS code_claim_party_ak_id,
	                code.claim_party_full_name            AS code_full_name,
	                code.claim_party_first_name           AS code_first_name,
	                code.claim_party_last_name            AS code_last_name,
	                code.claim_party_mid_name             AS code_mid_name,
	                code.claim_party_name_prfx            AS code_name_prfx,
	                code.claim_party_name_sfx             AS code_name_sfx,
	                wppp.claim_party_id                   AS wppp_claim_party_pk_id,
	                wppp.claim_party_ak_id                AS wppp_claim_party_ak_id,
	                wppp.claim_party_full_name            AS wppp_full_name,
	                wppp.claim_party_first_name           AS wppp_first_name,
	                wppp.claim_party_last_name            AS wppp_last_name,
	                wppp.claim_party_mid_name             AS wppp_mid_name,
	                wppp.claim_party_name_prfx            AS wppp_name_prfx,
	                wppp.claim_party_name_sfx             AS wppp_name_sfx,
	                plat.claim_party_id                   AS plat_claim_party_pk_id,
	                plat.claim_party_ak_id                AS plat_claim_party_ak_id,
	                plat.claim_party_full_name            AS plat_full_name,
	                plat.claim_party_first_name           AS plat_first_name,
	                plat.claim_party_last_name            AS plat_last_name,
	                plat.claim_party_mid_name             AS plat_mid_name,
	                plat.claim_party_name_prfx            AS plat_name_prfx,
	                plat.claim_party_name_sfx             AS plat_name_sfx,
	                iplt.claim_party_id                   AS iplt_claim_party_pk_id,
	                iplt.claim_party_ak_id                AS iplt_claim_party_ak_id,
	                iplt.claim_party_full_name            AS iplt_full_name,
	                iplt.claim_party_first_name           AS iplt_first_name,
	                iplt.claim_party_last_name            AS iplt_last_name,
	                iplt.claim_party_mid_name             AS iplt_mid_name,
	                iplt.claim_party_name_prfx            AS iplt_name_prfx,
	                iplt.claim_party_name_sfx             AS iplt_name_sfx
	FROM   (SELECT 'DEFD'                               AS defd_role_code,
	               'CODE'                               AS code_role_code,
	               'WPPP'                               AS wppp_role_code,
	               'PLAT'                               AS plat_role_code,
	               'IPLT'                               AS iplt_role_code,
	               defd.claim_party_relation_from_ak_id AS defd_claim_party_ak_id,
	               code.claim_party_relation_from_ak_id AS code_claim_party_ak_id,
	               wppp.claim_party_relation_from_ak_id AS wppp_claim_party_ak_id,
	               plat.claim_party_relation_from_ak_id AS plat_claim_party_ak_id,
	               iplt.claim_party_relation_from_ak_id AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id                   AS claim_case_ak_id,
	               a.eff_from_date                      AS eff_from_date,
	               c.prim_litigation_handler_ak_id      AS prim_litigation_handler_ak_id
	        FROM   claim_case c left outer join claim_party_occurrence a
	                 ON a.claim_case_ak_id = c.claim_case_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'DEFD'
	                                       AND source_sys_id = 'EXCEED') defd
	                 ON a.claim_party_occurrence_ak_id = defd.claim_party_occurrence_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'CODE'
	                                       AND source_sys_id = 'EXCEED') code
	                 ON a.claim_party_occurrence_ak_id = code.claim_party_occurrence_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'WPPP'
	                                       AND source_sys_id = 'EXCEED') wppp
	                 ON a.claim_party_occurrence_ak_id = wppp.claim_party_occurrence_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'PLAT'
	                                       AND source_sys_id = 'EXCEED') plat
	                 ON a.claim_party_occurrence_ak_id = plat.claim_party_occurrence_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'IPLT'
	                                       AND source_sys_id = 'EXCEED') iplt
	                 ON a.claim_party_occurrence_ak_id = iplt.claim_party_occurrence_ak_id
	--- Change to created_date 1 start
	        WHERE  a.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	               AND a.claim_party_role_code = 'CLMT'
	               AND a.source_sys_id = 'EXCEED'
	        UNION
	        SELECT 'DEFD'                               AS defd_role_code,
	               'CODE'                               AS code_role_code,
	               'WPPP'                               AS wppp_role_code,
	               'PLAT'                               AS plat_role_code,
	               'IPLT'                               AS iplt_role_code,
	               defd.claim_party_relation_from_ak_id AS defd_claim_party_ak_id,
	               code.claim_party_relation_from_ak_id AS code_claim_party_ak_id,
	               wppp.claim_party_relation_from_ak_id AS wppp_claim_party_ak_id,
	               plat.claim_party_relation_from_ak_id AS plat_claim_party_ak_id,
	               iplt.claim_party_relation_from_ak_id AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id                   AS claim_case_ak_id,
	               c.eff_from_date                      AS eff_from_date,
	               c.prim_litigation_handler_ak_id      AS prim_litigation_handler_ak_id
	        FROM   claim_case c left outer join claim_party_occurrence a
	               ON a.claim_case_ak_id = c.claim_case_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'DEFD'
	                                       AND source_sys_id = 'EXCEED') defd
	                 ON a.claim_party_occurrence_ak_id = defd.claim_party_occurrence_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'CODE'
	                                       AND source_sys_id = 'EXCEED') code
	                 ON a.claim_party_occurrence_ak_id = code.claim_party_occurrence_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'WPPP'
	                                       AND source_sys_id = 'EXCEED') wppp
	                 ON a.claim_party_occurrence_ak_id = wppp.claim_party_occurrence_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'PLAT'
	                                       AND source_sys_id = 'EXCEED') plat
	                 ON a.claim_party_occurrence_ak_id = plat.claim_party_occurrence_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'IPLT'
	                                       AND source_sys_id = 'EXCEED') iplt
	                 ON a.claim_party_occurrence_ak_id = iplt.claim_party_occurrence_ak_id
	--- Change to created_date 2 start
	        WHERE  c.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	               AND a.claim_party_role_code = 'CLMT'
	               AND a.source_sys_id = 'EXCEED'
	        UNION
	        SELECT 'DEFD'                               AS defd_role_code,
	               NULL                                 AS code_role_code,
	               NULL                                 AS wppp_role_code,
	               NULL                                 AS plat_role_code,
	               NULL                                 AS iplt_role_code,
	               defd.claim_party_relation_from_ak_id AS defd_claim_party_ak_id,
	               -1                                   AS code_claim_party_ak_id,
	               -1                                   AS wppp_claim_party_ak_id,
	               -1                                   AS plat_claim_party_ak_id,
	               -1                                   AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id                   AS claim_case_ak_id,
	               defd.eff_from_date                   AS eff_from_date,
	               c.prim_litigation_handler_ak_id      AS prim_litigation_handler_ak_id
	        FROM   claim_case c left outer join claim_party_occurrence a
	                 ON a.claim_case_ak_id = c.claim_case_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'DEFD'
	                                       AND source_sys_id = 'EXCEED') defd
	                 ON a.claim_party_occurrence_ak_id = defd.claim_party_occurrence_ak_id
	--- Change to created_date 3 start
	         WHERE  defd.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	               AND a.claim_party_role_code = 'CLMT'
	               AND a.source_sys_id = 'EXCEED'
	        UNION
	        SELECT NULL                                 AS defd_role_code,
	               'CODE'                               AS code_role_code,
	               NULL                                 AS wppp_role_code,
	               NULL                                 AS plat_role_code,
	               NULL                                 AS iplt_role_code,
	               -1                                   AS defd_claim_party_ak_id,
	               code.claim_party_relation_from_ak_id AS code_claim_party_ak_id,
	               -1                                   AS wppp_claim_party_ak_id,
	               -1                                   AS plat_claim_party_ak_id,
	               -1                                   AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id                   AS claim_case_ak_id,
	               code.eff_from_date                   AS eff_from_date,
	               c.prim_litigation_handler_ak_id      AS prim_litigation_handler_ak_id
	        FROM   claim_case c left outer join claim_party_occurrence a
	                 ON a.claim_case_ak_id = c.claim_case_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'CODE'
	                                       AND source_sys_id = 'EXCEED') code
	                 ON a.claim_party_occurrence_ak_id = code.claim_party_occurrence_ak_id
	--- Change to created_date 3 start      
	        WHERE  code.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	               AND a.claim_party_role_code = 'CLMT'
	               AND a.source_sys_id = 'EXCEED'
	        UNION
	        SELECT NULL                                 AS defd_role_code,
	               NULL                                 AS code_role_code,
	               'WPPP'                               AS wppp_role_code,
	               NULL                                 AS plat_role_code,
	               NULL                                 AS iplt_role_code,
	               -1                                   AS defd_claim_party_ak_id,
	               -1                                   AS code_claim_party_ak_id,
	               wppp.claim_party_relation_from_ak_id AS wppp_claim_party_ak_id,
	               -1                                   AS plat_claim_party_ak_id,
	               -1                                   AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id                   AS claim_case_ak_id,
	               wppp.eff_from_date                   AS eff_from_date,
	               c.prim_litigation_handler_ak_id      AS prim_litigation_handler_ak_id
	        FROM   claim_case c left outer join claim_party_occurrence a
	                 ON a.claim_case_ak_id = c.claim_case_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'WPPP'
	                                       AND source_sys_id = 'EXCEED') wppp
	                 ON a.claim_party_occurrence_ak_id = wppp.claim_party_occurrence_ak_id
	        --- Change to created_date 4 start      
	        WHERE  wppp.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	               AND a.claim_party_role_code = 'CLMT'
	               AND a.source_sys_id = 'EXCEED'
	        UNION
	        SELECT NULL                                 AS defd_role_code,
	               NULL                                 AS plat_role_code,
	               NULL                                 AS wppp_role_code,
	               'PLAT'                               AS code_role_code,
	               NULL                                 AS iplt_role_code,
	               -1                                   AS defd_claim_party_ak_id,
	               -1                                   AS code_claim_party_ak_id,
	               -1                                   AS wppp_claim_party_ak_id,
	               plat.claim_party_relation_from_ak_id AS plat_claim_party_ak_id,
	               -1                                   AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id                   AS claim_case_ak_id,
	               plat.eff_from_date                   AS eff_from_date,
	               c.prim_litigation_handler_ak_id      AS prim_litigation_handler_ak_id
	        FROM   claim_case c left outer join claim_party_occurrence a
	                 ON a.claim_case_ak_id = c.claim_case_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'PLAT'
	                                       AND source_sys_id = 'EXCEED') plat
	                 ON a.claim_party_occurrence_ak_id = plat.claim_party_occurrence_ak_id
	         --- Change to created_date 5 start 
	        WHERE  plat.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	               AND a.claim_party_role_code = 'CLMT'
	               AND a.source_sys_id = 'EXCEED'
	        UNION
	        SELECT NULL                                 AS defd_role_code,
	               NULL                                 AS code_role_code,
	               NULL                                 AS wppp_role_code,
	               NULL                                 AS plat_role_code,
	               'IPLT'                               AS iplt_role_code,
	               -1                                   AS defd_claim_party_ak_id,
	               -1                                   AS code_claim_party_ak_id,
	               -1                                   AS wppp_claim_party_ak_id,
	               -1                                   AS plat_claim_party_ak_id,
	               iplt.claim_party_relation_from_ak_id AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id                   AS claim_case_ak_id,
	               iplt.eff_from_date                   AS eff_from_date,
	               c.prim_litigation_handler_ak_id      AS prim_litigation_handler_ak_id
	        FROM   claim_case c left outer join claim_party_occurrence a
	                 ON a.claim_case_ak_id = c.claim_case_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_relation
	                                WHERE  claim_party_relation_role_code = 'IPLT'
	                                       AND source_sys_id = 'EXCEED') iplt
	                 ON a.claim_party_occurrence_ak_id = iplt.claim_party_occurrence_ak_id
	--- Change to created_date 6 start        
	        WHERE  iplt.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	               AND a.claim_party_role_code = 'CLMT'
	               AND a.source_sys_id = 'EXCEED'
	        UNION
	        SELECT 'DEFD'                          AS defd_role_code,
	               NULL                            AS code_role_code,
	               NULL                            AS wppp_role_code,
	               NULL                            AS plat_role_code,
	               NULL                            AS iplt_role_code,
	               defd.claim_party_ak_id          AS defd_claim_party_ak_id,
	               -1                              AS code_claim_party_ak_id,
	               -1                              AS wppp_claim_party_ak_id,
	               -1                              AS plat_claim_party_ak_id,
	               -1                              AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id              AS claim_case_ak_id,
	               defd.eff_from_date              AS eff_from_date,
	               c.prim_litigation_handler_ak_id AS prim_litigation_handler_ak_id
	        FROM   claim_case c left outer join (SELECT *
	                FROM   claim_party_occurrence
	                WHERE  claim_party_role_code = 'DEFD'
	                       AND source_sys_id = 'PMS') defd
	            ON c.claim_case_ak_id = defd.claim_case_ak_id
	--- Change to created_date 7 start        
	        WHERE  defd.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	        UNION
	        SELECT NULL                            AS defd_role_code,
	               'CODE'                          AS code_role_code,
	               NULL                            AS wppp_role_code,
	               NULL                            AS plat_role_code,
	               NULL                            AS iplt_role_code,
	               -1                              AS defd_claim_party_ak_id,
	               code.claim_party_ak_id          AS code_claim_party_ak_id,
	               -1                              AS wppp_claim_party_ak_id,
	               -1                              AS plat_claim_party_ak_id,
	               -1                              AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id              AS claim_case_ak_id,
	               code.eff_from_date              AS eff_from_date,
	               c.prim_litigation_handler_ak_id AS prim_litigation_handler_ak_id
	        FROM   claim_case c left outer join  (SELECT *
	                FROM   claim_party_occurrence
	                WHERE  claim_party_role_code = 'CODE'
	                       AND source_sys_id = 'PMS') code
	              ON c.claim_case_ak_id = code.claim_case_ak_id
	--- Change to created_date 8 start                
	        WHERE  code.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	        UNION
	        SELECT NULL                            AS defd_role_code,
	               NULL                            AS code_role_code,
	               'WPPP'                          AS wppp_role_code,
	               NULL                            AS plat_role_code,
	               NULL                            AS iplt_role_code,
	               -1                              AS defd_claim_party_ak_id,
	               -1                              AS code_claim_party_ak_id,
	               wppp.claim_party_ak_id          AS wppp_claim_party_ak_id,
	               -1                              AS plat_claim_party_ak_id,
	               -1                              AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id              AS claim_case_ak_id,
	               wppp.eff_from_date              AS eff_from_date,
	               c.prim_litigation_handler_ak_id AS prim_litigation_handler_ak_id
	        FROM   claim_case c left outer join  (SELECT *
	                FROM   claim_party_occurrence
	                WHERE  claim_party_role_code = 'WPPP'
	                       AND source_sys_id = 'PMS') wppp
	           ON c.claim_case_ak_id = wppp.claim_case_ak_id
	--- Change to created_date 9 start     
	        WHERE  wppp.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	        UNION
	        SELECT NULL                            AS defd_role_code,
	               NULL                            AS code_role_code,
	               NULL                            AS wppp_role_code,
	               'PLAT'                          AS plat_role_code,
	               NULL                            AS iplt_role_code,
	               -1                              AS defd_claim_party_ak_id,
	               -1                              AS code_claim_party_ak_id,
	               -1                              AS wppp_claim_party_ak_id,
	               plat.claim_party_ak_id          AS plat_claim_party_ak_id,
	               -1                              AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id              AS claim_case_ak_id,
	               plat.eff_from_date              AS eff_from_date,
	               c.prim_litigation_handler_ak_id AS prim_litigation_handler_ak_id
	        FROM  claim_case c left outer join   (SELECT *
	                FROM   claim_party_occurrence
	                WHERE  claim_party_role_code = 'PLAT'
	                       AND source_sys_id = 'PMS') plat
	         ON c.claim_case_ak_id = plat.claim_case_ak_id
	--- Change to created_date 10 start          
	        WHERE  plat.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	        UNION
	        SELECT NULL                            AS defd_role_code,
	               NULL                            AS code_role_code,
	               NULL                            AS wppp_role_code,
	               NULL                            AS plat_role_code,
	               'IPLT'                          AS iplt_role_code,
	               -1                              AS defd_claim_party_ak_id,
	               -1                              AS code_claim_party_ak_id,
	               -1                              AS wppp_claim_party_ak_id,
	               -1                              AS plat_claim_party_ak_id,
	               iplt.claim_party_ak_id          AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id              AS claim_case_ak_id,
	               iplt.eff_from_date              AS eff_from_date,
	               c.prim_litigation_handler_ak_id AS prim_litigation_handler_ak_id
	        FROM   claim_case c left outer join  (SELECT *
	                FROM   claim_party_occurrence
	                WHERE  claim_party_role_code = 'IPLT'
	                       AND source_sys_id = 'PMS') iplt
	        ON c.claim_case_ak_id = iplt.claim_case_ak_id
	--- Change to created_date 11 start                 
	        WHERE  iplt.created_date >= '@{pipeline().parameters.SELECTION_START_TS}'
	        UNION
	        SELECT 'DEFD'                          AS defd_role_code,
	               'CODE'                          AS code_role_code,
	               'WPPP'                          AS wppp_role_code,
	               'PLAT'                          AS plat_role_code,
	               'IPLT'                          AS iplt_role_code,
	               defd.claim_party_ak_id          AS defd_claim_party_ak_id,
	               code.claim_party_ak_id          AS code_claim_party_ak_id,
	               wppp.claim_party_ak_id          AS wppp_claim_party_ak_id,
	               plat.claim_party_ak_id          AS plat_claim_party_ak_id,
	               iplt.claim_party_ak_id          AS iplt_claim_party_ak_id,
	               c.claim_case_ak_id              AS claim_case_ak_id,
	               c.eff_from_date                 AS eff_from_date,
	               c.prim_litigation_handler_ak_id AS prim_litigation_handler_ak_id
	        FROM  claim_case c left outer join  (SELECT *
	                FROM   claim_party_occurrence
	                WHERE  claim_party_role_code = 'DEFD'
	                       AND source_sys_id = 'PMS') defd
	              ON c.claim_case_ak_id = defd.claim_case_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_occurrence
	                                WHERE  claim_party_role_code = 'CODE'
	                                       AND source_sys_id = 'PMS') code
	                 ON c.claim_case_ak_id = code.claim_case_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_occurrence
	                                WHERE  claim_party_role_code = 'WPPP'
	                                       AND source_sys_id = 'PMS') wppp
	                 ON c.claim_case_ak_id = wppp.claim_case_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_occurrence
	                                WHERE  claim_party_role_code = 'PLAT'
	                                       AND source_sys_id = 'PMS') plat
	                 ON c.claim_case_ak_id = plat.claim_case_ak_id
	               LEFT OUTER JOIN (SELECT *
	                                FROM   claim_party_occurrence
	                                WHERE  claim_party_role_code = 'IPLT'
	                                       AND source_sys_id = 'PMS') iplt
	                 ON c.claim_case_ak_id = iplt.claim_case_ak_id
	--- Change to created_date - 12 start                
	        
	        WHERE  c.created_date >= '@{pipeline().parameters.SELECTION_START_TS}') AS distinct_eff_from_dates
	       LEFT OUTER JOIN claim_party defd
	         ON distinct_eff_from_dates.defd_claim_party_ak_id = defd.claim_party_ak_id
	            AND distinct_eff_from_dates.eff_from_date BETWEEN defd.eff_from_date AND defd.eff_to_date
	       LEFT OUTER JOIN claim_party code
	         ON distinct_eff_from_dates.code_claim_party_ak_id = code.claim_party_ak_id
	            AND distinct_eff_from_dates.eff_from_date BETWEEN code.eff_from_date AND code.eff_to_date
	       LEFT OUTER JOIN claim_party wppp
	         ON distinct_eff_from_dates.wppp_claim_party_ak_id = wppp.claim_party_ak_id
	            AND distinct_eff_from_dates.eff_from_date BETWEEN wppp.eff_from_date AND wppp.eff_to_date
	       LEFT OUTER JOIN claim_party plat
	         ON distinct_eff_from_dates.plat_claim_party_ak_id = plat.claim_party_ak_id
	            AND distinct_eff_from_dates.eff_from_date BETWEEN plat.eff_from_date AND plat.eff_to_date
	       LEFT OUTER JOIN claim_party iplt
	         ON distinct_eff_from_dates.iplt_claim_party_ak_id = iplt.claim_party_ak_id
	            AND distinct_eff_from_dates.eff_from_date BETWEEN iplt.eff_from_date AND iplt.eff_to_date
	       LEFT OUTER JOIN claim_case cca
	         ON distinct_eff_from_dates.claim_case_ak_id = cca.claim_case_ak_id
	            AND distinct_eff_from_dates.eff_from_date BETWEEN cca.eff_from_date AND cca.eff_to_date
	       LEFT OUTER JOIN claim_party prim_lit
	         ON distinct_eff_from_dates.prim_litigation_handler_ak_id = prim_lit.claim_party_ak_id
	            AND distinct_eff_from_dates.eff_from_date BETWEEN prim_lit.eff_from_date AND prim_lit.eff_to_date
	--where cca.claim_case_ak_id  = 1263017
),
EXP_Default AS (
	SELECT
	claim_case_id,
	claim_case_ak_id,
	claim_case_key,
	claim_case_name,
	claim_case_num,
	suit_county,
	suit_state,
	trial_date,
	first_notice_law_suit_ind,
	declaratory_action_ind,
	suit_status_code,
	-- *INF*: iif(isnull(suit_status_code)
	-- ,'N/A'
	-- ,suit_status_code)
	IFF(suit_status_code IS NULL, 'N/A', suit_status_code) AS v_suit_status_code,
	v_suit_status_code AS suit_status_code_out,
	-- *INF*: iif(isnull(:LKP.LKP_CLAIM_SUIT_STATUS_CODE(v_suit_status_code))
	-- ,'N/A'
	-- ,:LKP.LKP_CLAIM_SUIT_STATUS_CODE(v_suit_status_code)
	-- )
	IFF(
	    LKP_CLAIM_SUIT_STATUS_CODE_v_suit_status_code.suit_status_code_descript IS NULL, 'N/A',
	    LKP_CLAIM_SUIT_STATUS_CODE_v_suit_status_code.suit_status_code_descript
	) AS suit_status_code_desc,
	suit_denial_date,
	prim_litigation_handler_role_code,
	-- *INF*: iif(isnull(prim_litigation_handler_role_code)
	-- ,'N/A'
	-- ,prim_litigation_handler_role_code)
	IFF(prim_litigation_handler_role_code IS NULL, 'N/A', prim_litigation_handler_role_code) AS v_prim_litigation_handler_role_code,
	v_prim_litigation_handler_role_code AS prim_litigation_handler_role_code_out,
	-- *INF*: iif(isnull(:LKP.LKP_PRIM_LITIGATION_HANDLER_ROLE_CODE(v_prim_litigation_handler_role_code))
	-- ,'N/A'
	-- ,:LKP.LKP_PRIM_LITIGATION_HANDLER_ROLE_CODE(v_prim_litigation_handler_role_code)
	-- )
	IFF(
	    LKP_PRIM_LITIGATION_HANDLER_ROLE_CODE_v_prim_litigation_handler_role_code.prim_litigation_role_code_descript IS NULL,
	    'N/A',
	    LKP_PRIM_LITIGATION_HANDLER_ROLE_CODE_v_prim_litigation_handler_role_code.prim_litigation_role_code_descript
	) AS prim_litigation_handler_role_code_desc,
	suit_open_date,
	suit_close_date,
	suit_how_claim_closed,
	reins_reported_ind,
	commercl_umb_reserve,
	suit_pay_amt,
	arbitration_open_date,
	arbitration_close_date,
	eff_from_date AS eff_from_date1,
	demand_at_initial_litigation,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	sysdate AS created_date,
	sysdate AS modified_date,
	claim_party_id AS ext_lit_pk_id,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_pk_id,-1)
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_pk_id,
	    - 1
	) AS o_ext_lit_pk_id,
	claim_party_ak_id AS ext_lit_ak_id,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_ak_id,-1)
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_ak_id,
	    - 1
	) AS o_ext_lit_ak_id,
	claim_party_full_name AS ext_lit_claim_party_full_name,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_claim_party_full_name,'N/A')
	-- 
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_claim_party_full_name,
	    'N/A'
	) AS o_ext_lit_claim_party_full_name,
	claim_party_first_name AS ext_lit_claim_party_first_name,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_claim_party_first_name,'N/A')
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_claim_party_first_name,
	    'N/A'
	) AS o_ext_lit_claim_party_first_name,
	claim_party_last_name AS ext_lit_claim_party_last_name,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- wppp_claim_party_last_name,'N/A')
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    wppp_claim_party_last_name,
	    'N/A'
	) AS o_ext_lit_claim_party_last_name,
	claim_party_mid_name AS ext_lit_claim_party_mid_name,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_claim_party_mid_name,'N/A')
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_claim_party_mid_name,
	    'N/A'
	) AS o_ext_lit_claim_party_mid_name,
	claim_party_name_prfx AS ext_lit_claim_party_name_prfx,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_claim_party_name_prfx,'N/A')
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_claim_party_name_prfx,
	    'N/A'
	) AS o_ext_lit_claim_party_name_prfx,
	claim_party_name_sfx AS ext_lit_claim_party_name_sfx,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_claim_party_name_sfx,'N/A')
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_claim_party_name_sfx,
	    'N/A'
	) AS o_ext_lit_claim_party_name_sfx,
	claim_party_addr AS ext_lit_claim_party_addr,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_claim_party_addr,'N/A')
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_claim_party_addr,
	    'N/A'
	) AS o_ext_lit_claim_party_addr,
	claim_party_city AS ext_lit_claim_party_city,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_claim_party_city,'N/A')
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_claim_party_city,
	    'N/A'
	) AS o_ext_lit_claim_party_city,
	claim_party_county AS ext_lit_claim_party_county,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_claim_party_county,'N/A')
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_claim_party_county,
	    'N/A'
	) AS o_ext_lit_claim_party_county,
	claim_party_state AS ext_lit_claim_party_state,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_claim_party_state,'N/A')
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_claim_party_state,
	    'N/A'
	) AS o_ext_lit_claim_party_state,
	claim_party_zip AS ext_lit_claim_party_zip,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_claim_party_zip,'N/A')
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_claim_party_zip,
	    'N/A'
	) AS o_ext_lit_claim_party_zip,
	addr_type AS ext_lit_addr_type,
	-- *INF*: iif((v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	-- ext_lit_addr_type,'N/A')
	IFF(
	    (v_prim_litigation_handler_role_code = 'REG' OR v_prim_litigation_handler_role_code = 'ARB'),
	    ext_lit_addr_type,
	    'N/A'
	) AS o_ext_lit_addr_type,
	claim_party_id1 AS def_claim_party_id,
	claim_party_ak_id2 AS def_claim_party_ak_id,
	claim_party_full_name1 AS defd_claim_party_full_name1,
	claim_party_first_name1 AS defd_claim_party_first_name1,
	claim_party_last_name1 AS defd_claim_party_last_name1,
	claim_party_mid_name1 AS defd_claim_party_mid_name1,
	claim_party_name_prfx1 AS defd_claim_party_name_prfx1,
	claim_party_name_sfx1 AS defd_claim_party_name_sfx1,
	claim_party_id2 AS code_claim_party_id,
	claim_party_ak_id3 AS code_claim_party_ak_id,
	claim_party_full_name2 AS code_claim_party_full_name2,
	claim_party_first_name2 AS code_claim_party_first_name2,
	claim_party_last_name2 AS code_claim_party_last_name2,
	claim_party_mid_name2 AS code_claim_party_mid_name2,
	claim_party_name_prfx2 AS code_claim_party_name_prfx2,
	claim_party_name_sfx2 AS code_claim_party_name_sfx2,
	-- *INF*: iif(isnull(def_claim_party_id)
	-- ,code_claim_party_id
	-- ,def_claim_party_id)
	IFF(def_claim_party_id IS NULL, code_claim_party_id, def_claim_party_id) AS o_defd_claim_party_id,
	-- *INF*: iif(isnull(def_claim_party_id)
	-- ,code_claim_party_ak_id
	-- ,def_claim_party_ak_id)
	IFF(def_claim_party_id IS NULL, code_claim_party_ak_id, def_claim_party_ak_id) AS o_defd_claim_party_ak_id,
	-- *INF*: iif(isnull(def_claim_party_id)
	-- ,code_claim_party_full_name2
	-- ,defd_claim_party_full_name1)
	IFF(def_claim_party_id IS NULL, code_claim_party_full_name2, defd_claim_party_full_name1) AS o_defd_claim_party_full_Name,
	-- *INF*: iif(isnull(def_claim_party_id)
	-- ,code_claim_party_first_name2
	-- ,defd_claim_party_first_name1)
	IFF(def_claim_party_id IS NULL, code_claim_party_first_name2, defd_claim_party_first_name1) AS o_defd_claim_party_first_name,
	-- *INF*: iif(isnull(def_claim_party_id)
	-- ,code_claim_party_last_name2
	-- ,defd_claim_party_last_name1)
	IFF(def_claim_party_id IS NULL, code_claim_party_last_name2, defd_claim_party_last_name1) AS o_defd_claim_party_last_name,
	-- *INF*: iif(isnull(def_claim_party_id)
	-- ,code_claim_party_mid_name2
	-- ,defd_claim_party_mid_name1)
	IFF(def_claim_party_id IS NULL, code_claim_party_mid_name2, defd_claim_party_mid_name1) AS o_defd_claim_party_mid_name,
	-- *INF*: iif(isnull(def_claim_party_id)
	-- ,code_claim_party_name_prfx2
	-- ,defd_claim_party_name_prfx1)
	IFF(def_claim_party_id IS NULL, code_claim_party_name_prfx2, defd_claim_party_name_prfx1) AS o_defd_claim_party_name_prfx,
	-- *INF*: iif(isnull(def_claim_party_id)
	-- ,code_claim_party_name_sfx2
	-- ,defd_claim_party_name_sfx1)
	IFF(def_claim_party_id IS NULL, code_claim_party_name_sfx2, defd_claim_party_name_sfx1) AS o_defd_claim_party_name_sfx,
	claim_party_id5 AS wppp_claim_party_id,
	claim_party_ak_id6 AS wppp_claim_party_ak_id,
	claim_party_full_name5 AS wppp_claim_party_full_name,
	claim_party_first_name5 AS wppp_claim_party_first_name,
	claim_party_last_name5 AS wppp_claim_party_last_name,
	claim_party_mid_name5 AS wppp_claim_party_mid_name,
	claim_party_name_prfx5 AS wppp_claim_party_name_prfx,
	claim_party_name_sfx5 AS wppp_claim_party_name_sfx,
	claim_party_id3 AS plat_claim_party_id,
	claim_party_ak_id4 AS plat_claim_party_ak_id,
	claim_party_full_name3 AS plat_claim_party_full_name3,
	claim_party_first_name3 AS plat_claim_party_first_name3,
	claim_party_last_name3 AS plat_claim_party_last_name3,
	claim_party_mid_name3 AS plat_claim_party_mid_name3,
	claim_party_name_prfx3 AS plat_claim_party_name_prfx3,
	claim_party_name_sfx3 AS plat_claim_party_name_sfx3,
	claim_party_id4 AS iplt_claim_party_id,
	claim_party_ak_id5 AS iplt_claim_party_ak_id,
	claim_party_full_name4 AS iplt_claim_party_full_name4,
	claim_party_first_name4 AS iplt_claim_party_first_name4,
	claim_party_last_name4 AS iplt_claim_party_last_name4,
	claim_party_mid_name4 AS iplt_claim_party_mid_name4,
	claim_party_name_prfx4 AS iplt_claim_party_name_prfx4,
	claim_party_name_sfx4 AS iplt_claim_party_name_sfx4,
	-- *INF*: iif(not isnull(wppp_claim_party_id)
	-- 	,wppp_claim_party_id
	-- 	,iif(isnull(plat_claim_party_id)
	-- 			,iplt_claim_party_id
	-- 			,plat_claim_party_id)
	--    )
	-- 
	IFF(
	    wppp_claim_party_id IS NOT NULL, wppp_claim_party_id,
	    IFF(
	        plat_claim_party_id IS NULL, iplt_claim_party_id, plat_claim_party_id
	    )
	) AS o_plat_claim_party_id,
	-- *INF*: iif(not isnull(wppp_claim_party_id)
	-- ,wppp_claim_party_ak_id
	-- ,iif(isnull(plat_claim_party_id)
	-- ,iplt_claim_party_ak_id
	-- ,plat_claim_party_ak_id)
	-- )
	IFF(
	    wppp_claim_party_id IS NOT NULL, wppp_claim_party_ak_id,
	    IFF(
	        plat_claim_party_id IS NULL, iplt_claim_party_ak_id, plat_claim_party_ak_id
	    )
	) AS o_plat_claim_party_ak_id,
	-- *INF*: iif(not isnull(wppp_claim_party_id)
	-- ,wppp_claim_party_full_name
	-- ,iif(isnull(plat_claim_party_id)
	-- ,iplt_claim_party_full_name4
	-- ,plat_claim_party_full_name3)
	-- )
	IFF(
	    wppp_claim_party_id IS NOT NULL, wppp_claim_party_full_name,
	    IFF(
	        plat_claim_party_id IS NULL, iplt_claim_party_full_name4,
	        plat_claim_party_full_name3
	    )
	) AS o_plat_claim_party_full_name,
	-- *INF*: iif(not isnull(wppp_claim_party_id)
	-- ,wppp_claim_party_first_name
	-- ,iif(isnull(plat_claim_party_id)
	-- ,iplt_claim_party_first_name4
	-- ,plat_claim_party_first_name3)
	-- )
	IFF(
	    wppp_claim_party_id IS NOT NULL, wppp_claim_party_first_name,
	    IFF(
	        plat_claim_party_id IS NULL, iplt_claim_party_first_name4,
	        plat_claim_party_first_name3
	    )
	) AS o_plat_claim_party_first_name,
	-- *INF*: iif(not isnull(wppp_claim_party_id)
	-- ,wppp_claim_party_last_name
	-- ,iif(isnull(plat_claim_party_id)
	-- ,iplt_claim_party_last_name4
	-- ,plat_claim_party_last_name3)
	-- )
	IFF(
	    wppp_claim_party_id IS NOT NULL, wppp_claim_party_last_name,
	    IFF(
	        plat_claim_party_id IS NULL, iplt_claim_party_last_name4,
	        plat_claim_party_last_name3
	    )
	) AS o_plat_claim_party_last_name,
	-- *INF*: iif(not isnull(wppp_claim_party_id)
	-- ,wppp_claim_party_mid_name
	-- ,iif(isnull(plat_claim_party_id)
	-- ,iplt_claim_party_mid_name4
	-- ,plat_claim_party_mid_name3)
	-- )
	IFF(
	    wppp_claim_party_id IS NOT NULL, wppp_claim_party_mid_name,
	    IFF(
	        plat_claim_party_id IS NULL, iplt_claim_party_mid_name4, plat_claim_party_mid_name3
	    )
	) AS o_plat_claim_party_mid_name,
	-- *INF*: iif(not isnull(wppp_claim_party_id)
	-- ,wppp_claim_party_name_prfx
	-- ,iif(isnull(plat_claim_party_id)
	-- ,iplt_claim_party_name_prfx4
	-- ,plat_claim_party_name_prfx3)
	-- )
	IFF(
	    wppp_claim_party_id IS NOT NULL, wppp_claim_party_name_prfx,
	    IFF(
	        plat_claim_party_id IS NULL, iplt_claim_party_name_prfx4,
	        plat_claim_party_name_prfx3
	    )
	) AS o_plat_claim_party_name_prfx,
	-- *INF*: iif(not isnull(wppp_claim_party_id)
	-- ,wppp_claim_party_name_sfx
	-- ,iif(isnull(plat_claim_party_id)
	-- ,iplt_claim_party_name_sfx4
	-- ,plat_claim_party_name_sfx3)
	-- )
	IFF(
	    wppp_claim_party_id IS NOT NULL, wppp_claim_party_name_sfx,
	    IFF(
	        plat_claim_party_id IS NULL, iplt_claim_party_name_sfx4, plat_claim_party_name_sfx3
	    )
	) AS o_plat_claim_party_name_sfx
	FROM SQ_claim_case
	LEFT JOIN LKP_CLAIM_SUIT_STATUS_CODE LKP_CLAIM_SUIT_STATUS_CODE_v_suit_status_code
	ON LKP_CLAIM_SUIT_STATUS_CODE_v_suit_status_code.suit_status_code = v_suit_status_code

	LEFT JOIN LKP_PRIM_LITIGATION_HANDLER_ROLE_CODE LKP_PRIM_LITIGATION_HANDLER_ROLE_CODE_v_prim_litigation_handler_role_code
	ON LKP_PRIM_LITIGATION_HANDLER_ROLE_CODE_v_prim_litigation_handler_role_code.prim_litigation_role_code = v_prim_litigation_handler_role_code

),
FLT_Nulls_Claim_Cases AS (
	SELECT
	claim_case_id, 
	claim_case_ak_id, 
	claim_case_key, 
	claim_case_name, 
	claim_case_num, 
	suit_county, 
	suit_state, 
	trial_date, 
	first_notice_law_suit_ind, 
	declaratory_action_ind, 
	suit_status_code_out AS suit_status_code, 
	suit_status_code_desc, 
	suit_denial_date, 
	prim_litigation_handler_role_code_out AS prim_litigation_handler_role_code, 
	prim_litigation_handler_role_code_desc, 
	suit_open_date, 
	suit_close_date, 
	suit_how_claim_closed, 
	reins_reported_ind, 
	commercl_umb_reserve, 
	suit_pay_amt, 
	arbitration_open_date, 
	arbitration_close_date, 
	demand_at_initial_litigation, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date1 AS eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	o_ext_lit_pk_id AS ext_lit_pk_id, 
	o_ext_lit_ak_id AS ext_lit_ak_id, 
	o_ext_lit_claim_party_full_name AS ext_lit_claim_party_full_name, 
	o_ext_lit_claim_party_first_name AS ext_lit_claim_party_first_name, 
	o_ext_lit_claim_party_last_name AS ext_lit_claim_party_last_name, 
	o_ext_lit_claim_party_mid_name AS ext_lit_claim_party_mid_name, 
	o_ext_lit_claim_party_name_prfx AS ext_lit_claim_party_name_prfx, 
	o_ext_lit_claim_party_name_sfx AS ext_lit_claim_party_name_sfx, 
	o_ext_lit_claim_party_addr AS ext_lit_claim_party_addr, 
	o_ext_lit_claim_party_city AS ext_lit_claim_party_city, 
	o_ext_lit_claim_party_county AS ext_lit_claim_party_county, 
	o_ext_lit_claim_party_state AS ext_lit_claim_party_state, 
	o_ext_lit_claim_party_zip AS ext_lit_claim_party_zip, 
	o_ext_lit_addr_type AS ext_lit_addr_type, 
	o_defd_claim_party_id, 
	o_defd_claim_party_ak_id, 
	o_defd_claim_party_full_Name, 
	o_defd_claim_party_first_name, 
	o_defd_claim_party_last_name, 
	o_defd_claim_party_mid_name, 
	o_defd_claim_party_name_prfx, 
	o_defd_claim_party_name_sfx, 
	o_plat_claim_party_id, 
	o_plat_claim_party_ak_id, 
	plat_client_seq_nbr, 
	o_plat_claim_party_full_name, 
	o_plat_claim_party_first_name, 
	o_plat_claim_party_last_name, 
	o_plat_claim_party_mid_name, 
	o_plat_claim_party_name_prfx, 
	o_plat_claim_party_name_sfx
	FROM EXP_Default
	WHERE not isnull(claim_case_ak_id) --and ext_lit_pk_id <> -1
),
SRT_Sort_Plaintiffs AS (
	SELECT
	claim_case_ak_id, 
	eff_from_date, 
	o_plat_claim_party_ak_id, 
	claim_case_id, 
	claim_case_key, 
	claim_case_name, 
	claim_case_num, 
	suit_county, 
	suit_state, 
	trial_date, 
	first_notice_law_suit_ind, 
	declaratory_action_ind, 
	suit_status_code, 
	suit_status_code_desc, 
	suit_denial_date, 
	prim_litigation_handler_role_code, 
	prim_litigation_handler_role_code_desc, 
	suit_open_date, 
	suit_close_date, 
	suit_how_claim_closed, 
	reins_reported_ind, 
	commercl_umb_reserve, 
	suit_pay_amt, 
	arbitration_open_date, 
	arbitration_close_date, 
	demand_at_initial_litigation, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_to_date, 
	created_date, 
	modified_date, 
	ext_lit_pk_id, 
	ext_lit_ak_id, 
	ext_lit_claim_party_full_name, 
	ext_lit_claim_party_first_name, 
	ext_lit_claim_party_last_name, 
	ext_lit_claim_party_mid_name, 
	ext_lit_claim_party_name_prfx, 
	ext_lit_claim_party_name_sfx, 
	ext_lit_claim_party_addr, 
	ext_lit_claim_party_city, 
	ext_lit_claim_party_county, 
	ext_lit_claim_party_state, 
	ext_lit_claim_party_zip, 
	ext_lit_addr_type, 
	o_plat_claim_party_id, 
	o_plat_claim_party_full_name, 
	o_plat_claim_party_first_name, 
	o_plat_claim_party_last_name, 
	o_plat_claim_party_mid_name, 
	o_plat_claim_party_name_prfx, 
	o_plat_claim_party_name_sfx
	FROM FLT_Nulls_Claim_Cases
	ORDER BY claim_case_ak_id ASC, eff_from_date ASC, o_plat_claim_party_ak_id ASC
),
Agg_Plaintiff_Data AS (
	SELECT
	claim_case_id,
	claim_case_ak_id,
	claim_case_key,
	claim_case_name,
	claim_case_num,
	suit_county,
	suit_state,
	trial_date,
	first_notice_law_suit_ind,
	declaratory_action_ind,
	suit_status_code,
	suit_status_code_desc,
	suit_denial_date,
	prim_litigation_handler_role_code,
	prim_litigation_handler_role_code_desc,
	suit_open_date,
	suit_close_date,
	suit_how_claim_closed,
	reins_reported_ind,
	commercl_umb_reserve,
	suit_pay_amt,
	arbitration_open_date,
	arbitration_close_date,
	demand_at_initial_litigation,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date,
	ext_lit_pk_id,
	ext_lit_ak_id,
	ext_lit_claim_party_full_name,
	ext_lit_claim_party_first_name,
	ext_lit_claim_party_last_name,
	ext_lit_claim_party_mid_name,
	ext_lit_claim_party_name_prfx,
	ext_lit_claim_party_name_sfx,
	ext_lit_claim_party_addr,
	ext_lit_claim_party_city,
	ext_lit_claim_party_county,
	ext_lit_claim_party_state,
	ext_lit_claim_party_zip,
	ext_lit_addr_type,
	o_plat_claim_party_id AS plat_claim_party_id,
	-- *INF*: iif(isnull(first(plat_claim_party_id,plat_claim_party_id != -1))
	-- ,-1
	-- ,first(plat_claim_party_id,plat_claim_party_id != -1))
	IFF(
	    first(plat_claim_party_id, plat_claim_party_id != - 1) IS NULL, - 1,
	    first(plat_claim_party_id, plat_claim_party_id != - 1)
	) AS o_plat_claim_party_id,
	o_plat_claim_party_ak_id AS plat_claim_party_ak_id,
	-- *INF*: iif(isnull(first(plat_claim_party_ak_id,plat_claim_party_ak_id != -1))
	-- ,-1
	-- ,first(plat_claim_party_ak_id,plat_claim_party_ak_id != -1))
	IFF(
	    first(plat_claim_party_ak_id, plat_claim_party_ak_id != - 1) IS NULL, - 1,
	    first(plat_claim_party_ak_id, plat_claim_party_ak_id != - 1)
	) AS o_plat_claim_party_ak_id,
	o_plat_claim_party_full_name AS plat_claim_party_full_name,
	-- *INF*: iif(isnull(first(plat_claim_party_full_name,plat_claim_party_full_name != 'N/A'))
	-- ,'N/A'
	-- ,first(plat_claim_party_full_name,plat_claim_party_full_name != 'N/A'))
	IFF(
	    first(plat_claim_party_full_name, plat_claim_party_full_name != 'N/A') IS NULL, 'N/A',
	    first(plat_claim_party_full_name, plat_claim_party_full_name != 'N/A')
	) AS o_plat_claim_party_full_name,
	o_plat_claim_party_first_name AS plat_claim_party_first_name,
	-- *INF*: iif(isnull(first(plat_claim_party_first_name,plat_claim_party_first_name  != 'N/A'))
	-- ,'N/A'
	-- ,first(plat_claim_party_first_name,plat_claim_party_first_name  != 'N/A'))
	IFF(
	    first(plat_claim_party_first_name, plat_claim_party_first_name != 'N/A') IS NULL, 'N/A',
	    first(plat_claim_party_first_name, plat_claim_party_first_name != 'N/A')
	) AS o_plat_claim_party_first_name,
	o_plat_claim_party_last_name AS plat_claim_party_last_name,
	-- *INF*: iif(isnull(first(plat_claim_party_last_name,plat_claim_party_last_name != 'N/A'))
	-- ,'N/A'
	-- ,first(plat_claim_party_last_name,plat_claim_party_last_name != 'N/A'))
	IFF(
	    first(plat_claim_party_last_name, plat_claim_party_last_name != 'N/A') IS NULL, 'N/A',
	    first(plat_claim_party_last_name, plat_claim_party_last_name != 'N/A')
	) AS o_plat_claim_party_last_name,
	o_plat_claim_party_mid_name AS plat_claim_party_mid_name,
	-- *INF*: iif(isnull(first(plat_claim_party_mid_name,plat_claim_party_mid_name  != 'N/A'))
	-- ,'N/A'
	-- ,first(plat_claim_party_mid_name,plat_claim_party_mid_name  != 'N/A'))
	IFF(
	    first(plat_claim_party_mid_name, plat_claim_party_mid_name != 'N/A') IS NULL, 'N/A',
	    first(plat_claim_party_mid_name, plat_claim_party_mid_name != 'N/A')
	) AS o_plat_claim_party_mid_name,
	o_plat_claim_party_name_prfx AS plat_claim_party_name_prfx,
	-- *INF*: iif(isnull(first(plat_claim_party_name_prfx,plat_claim_party_name_prfx != 'N/A'))
	-- ,'N/A'
	-- ,first(plat_claim_party_name_prfx,plat_claim_party_name_prfx != 'N/A'))
	IFF(
	    first(plat_claim_party_name_prfx, plat_claim_party_name_prfx != 'N/A') IS NULL, 'N/A',
	    first(plat_claim_party_name_prfx, plat_claim_party_name_prfx != 'N/A')
	) AS o_plat_claim_party_name_prfx,
	o_plat_claim_party_name_sfx AS plat_claim_party_name_sfx,
	-- *INF*: iif(isnull(first(plat_claim_party_name_sfx,plat_claim_party_name_sfx != 'N/A'))
	-- ,'N/A'
	-- ,first(plat_claim_party_name_sfx,plat_claim_party_name_sfx != 'N/A'))
	IFF(
	    first(plat_claim_party_name_sfx, plat_claim_party_name_sfx != 'N/A') IS NULL, 'N/A',
	    first(plat_claim_party_name_sfx, plat_claim_party_name_sfx != 'N/A')
	) AS o_plat_claim_party_name_sfx
	FROM SRT_Sort_Plaintiffs
	GROUP BY claim_case_ak_id, eff_from_date
),
SR2 AS (
	SELECT
	claim_case_id, 
	claim_case_ak_id, 
	claim_case_key, 
	claim_case_name, 
	claim_case_num, 
	suit_county, 
	suit_state, 
	trial_date, 
	first_notice_law_suit_ind, 
	declaratory_action_ind, 
	suit_status_code, 
	suit_status_code_desc, 
	suit_denial_date, 
	prim_litigation_handler_role_code, 
	prim_litigation_handler_role_code_desc, 
	suit_open_date, 
	suit_close_date, 
	suit_how_claim_closed, 
	reins_reported_ind, 
	commercl_umb_reserve, 
	suit_pay_amt, 
	arbitration_open_date, 
	arbitration_close_date, 
	demand_at_initial_litigation, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	ext_lit_pk_id, 
	ext_lit_ak_id, 
	ext_lit_claim_party_full_name, 
	ext_lit_claim_party_first_name, 
	ext_lit_claim_party_last_name, 
	ext_lit_claim_party_mid_name, 
	ext_lit_claim_party_name_prfx, 
	ext_lit_claim_party_name_sfx, 
	ext_lit_claim_party_addr, 
	ext_lit_claim_party_city, 
	ext_lit_claim_party_county, 
	ext_lit_claim_party_state, 
	ext_lit_claim_party_zip, 
	ext_lit_addr_type, 
	o_plat_claim_party_id, 
	o_plat_claim_party_ak_id, 
	o_plat_claim_party_full_name, 
	o_plat_claim_party_first_name, 
	o_plat_claim_party_last_name, 
	o_plat_claim_party_mid_name, 
	o_plat_claim_party_name_prfx, 
	o_plat_claim_party_name_sfx
	FROM Agg_Plaintiff_Data
	ORDER BY claim_case_ak_id ASC, eff_from_date ASC
),
SRT_Sort_Defendants AS (
	SELECT
	claim_case_ak_id, 
	eff_from_date, 
	o_defd_claim_party_ak_id, 
	claim_case_id, 
	claim_case_key, 
	claim_case_name, 
	claim_case_num, 
	suit_county, 
	suit_state, 
	trial_date, 
	first_notice_law_suit_ind, 
	declaratory_action_ind, 
	suit_status_code, 
	suit_status_code_desc, 
	suit_denial_date, 
	prim_litigation_handler_role_code, 
	prim_litigation_handler_role_code_desc, 
	suit_open_date, 
	suit_close_date, 
	suit_how_claim_closed, 
	reins_reported_ind, 
	commercl_umb_reserve, 
	suit_pay_amt, 
	arbitration_open_date, 
	arbitration_close_date, 
	demand_at_initial_litigation, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_to_date, 
	created_date, 
	modified_date, 
	ext_lit_pk_id, 
	ext_lit_ak_id, 
	ext_lit_claim_party_full_name, 
	ext_lit_claim_party_first_name, 
	ext_lit_claim_party_last_name, 
	ext_lit_claim_party_mid_name, 
	ext_lit_claim_party_name_prfx, 
	ext_lit_claim_party_name_sfx, 
	ext_lit_claim_party_addr, 
	ext_lit_claim_party_city, 
	ext_lit_claim_party_county, 
	ext_lit_claim_party_state, 
	ext_lit_claim_party_zip, 
	ext_lit_addr_type, 
	o_defd_claim_party_id, 
	o_defd_claim_party_full_Name, 
	o_defd_claim_party_first_name, 
	o_defd_claim_party_last_name, 
	o_defd_claim_party_mid_name, 
	o_defd_claim_party_name_prfx, 
	o_defd_claim_party_name_sfx
	FROM FLT_Nulls_Claim_Cases
	ORDER BY claim_case_ak_id ASC, eff_from_date ASC, o_defd_claim_party_ak_id ASC
),
Agg_Defendant_Data AS (
	SELECT
	claim_case_id,
	claim_case_ak_id,
	claim_case_key,
	claim_case_name,
	claim_case_num,
	suit_county,
	suit_state,
	trial_date,
	first_notice_law_suit_ind,
	declaratory_action_ind,
	suit_status_code,
	suit_status_code_desc,
	suit_denial_date,
	prim_litigation_handler_role_code,
	prim_litigation_handler_role_code_desc,
	suit_open_date,
	suit_close_date,
	suit_how_claim_closed,
	reins_reported_ind,
	commercl_umb_reserve,
	suit_pay_amt,
	arbitration_open_date,
	arbitration_close_date,
	demand_at_initial_litigation,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	created_date,
	modified_date,
	ext_lit_pk_id,
	ext_lit_ak_id,
	ext_lit_claim_party_full_name,
	ext_lit_claim_party_first_name,
	ext_lit_claim_party_last_name,
	ext_lit_claim_party_mid_name,
	ext_lit_claim_party_name_prfx,
	ext_lit_claim_party_name_sfx,
	ext_lit_claim_party_addr,
	ext_lit_claim_party_city,
	ext_lit_claim_party_county,
	ext_lit_claim_party_state,
	ext_lit_claim_party_zip,
	ext_lit_addr_type,
	o_defd_claim_party_id AS defd_claim_party_id,
	-- *INF*: iif(isnull(first(defd_claim_party_id,defd_claim_party_id != -1))
	-- ,-1
	-- ,first(defd_claim_party_id,defd_claim_party_id != -1)
	-- )
	IFF(
	    first(defd_claim_party_id, defd_claim_party_id != - 1) IS NULL, - 1,
	    first(defd_claim_party_id, defd_claim_party_id != - 1)
	) AS o_defd_claim_party_id,
	o_defd_claim_party_ak_id AS defd_claim_party_ak_id,
	-- *INF*: iif(isnull(first(defd_claim_party_ak_id, defd_claim_party_ak_id != -1))
	-- ,-1
	-- ,first(defd_claim_party_ak_id, defd_claim_party_ak_id != -1)
	-- )
	IFF(
	    first(defd_claim_party_ak_id, defd_claim_party_ak_id != - 1) IS NULL, - 1,
	    first(defd_claim_party_ak_id, defd_claim_party_ak_id != - 1)
	) AS o_defd_claim_party_ak_id,
	o_defd_claim_party_full_Name AS defd_claim_party_full_Name,
	-- *INF*: iif(isnull(first(defd_claim_party_full_Name, defd_claim_party_full_Name != 'N/A'))
	-- ,'N/A'
	-- ,first(defd_claim_party_full_Name, defd_claim_party_full_Name != 'N/A')
	-- )
	IFF(
	    first(defd_claim_party_full_Name, defd_claim_party_full_Name != 'N/A') IS NULL, 'N/A',
	    first(defd_claim_party_full_Name, defd_claim_party_full_Name != 'N/A')
	) AS o_defd_claim_party_full_Name,
	o_defd_claim_party_first_name AS defd_claim_party_first_name,
	-- *INF*: iif(isnull(first(defd_claim_party_first_name,defd_claim_party_first_name != 'N/A'))
	-- ,'N/A'
	-- ,first(defd_claim_party_first_name,defd_claim_party_first_name != 'N/A'))
	IFF(
	    first(defd_claim_party_first_name, defd_claim_party_first_name != 'N/A') IS NULL, 'N/A',
	    first(defd_claim_party_first_name, defd_claim_party_first_name != 'N/A')
	) AS o_defd_claim_party_first_name,
	o_defd_claim_party_last_name AS defd_claim_party_last_name,
	-- *INF*: iif(isnull(first(defd_claim_party_last_name,defd_claim_party_last_name != 'N/A'))
	-- ,'N/A'
	-- ,first(defd_claim_party_last_name,defd_claim_party_last_name != 'N/A'))
	IFF(
	    first(defd_claim_party_last_name, defd_claim_party_last_name != 'N/A') IS NULL, 'N/A',
	    first(defd_claim_party_last_name, defd_claim_party_last_name != 'N/A')
	) AS o_defd_claim_party_last_name,
	o_defd_claim_party_mid_name AS defd_claim_party_mid_name,
	-- *INF*: iif(isnull(first(defd_claim_party_mid_name,defd_claim_party_mid_name != 'N/A'))
	-- ,'N/A'
	-- ,first(defd_claim_party_mid_name,defd_claim_party_mid_name != 'N/A'))
	IFF(
	    first(defd_claim_party_mid_name, defd_claim_party_mid_name != 'N/A') IS NULL, 'N/A',
	    first(defd_claim_party_mid_name, defd_claim_party_mid_name != 'N/A')
	) AS o_defd_claim_party_mid_name,
	o_defd_claim_party_name_prfx AS defd_claim_party_name_prfx,
	-- *INF*: iif(isnull(first(defd_claim_party_name_prfx,defd_claim_party_name_prfx != 'N/A'))
	-- ,'N/A'
	-- ,first(defd_claim_party_name_prfx,defd_claim_party_name_prfx != 'N/A'))
	IFF(
	    first(defd_claim_party_name_prfx, defd_claim_party_name_prfx != 'N/A') IS NULL, 'N/A',
	    first(defd_claim_party_name_prfx, defd_claim_party_name_prfx != 'N/A')
	) AS o_defd_claim_party_name_prfx,
	o_defd_claim_party_name_sfx AS defd_claim_party_name_sfx,
	-- *INF*: iif(isnull(first(defd_claim_party_name_sfx,defd_claim_party_name_sfx != 'N/A'))
	-- ,'N/A'
	-- ,first(defd_claim_party_name_sfx,defd_claim_party_name_sfx != 'N/A'))
	IFF(
	    first(defd_claim_party_name_sfx, defd_claim_party_name_sfx != 'N/A') IS NULL, 'N/A',
	    first(defd_claim_party_name_sfx, defd_claim_party_name_sfx != 'N/A')
	) AS o_defd_claim_party_name_sfx
	FROM SRT_Sort_Defendants
	GROUP BY claim_case_ak_id, eff_from_date
),
SRT1 AS (
	SELECT
	claim_case_id, 
	claim_case_ak_id, 
	claim_case_key, 
	claim_case_name, 
	claim_case_num, 
	suit_county, 
	suit_state, 
	trial_date, 
	first_notice_law_suit_ind, 
	declaratory_action_ind, 
	suit_status_code, 
	suit_status_code_desc, 
	suit_denial_date, 
	prim_litigation_handler_role_code, 
	prim_litigation_handler_role_code_desc, 
	suit_open_date, 
	suit_close_date, 
	suit_how_claim_closed, 
	reins_reported_ind, 
	commercl_umb_reserve, 
	suit_pay_amt, 
	arbitration_open_date, 
	arbitration_close_date, 
	demand_at_initial_litigation, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	created_date, 
	modified_date, 
	ext_lit_pk_id, 
	ext_lit_ak_id, 
	ext_lit_claim_party_full_name, 
	ext_lit_claim_party_first_name, 
	ext_lit_claim_party_last_name, 
	ext_lit_claim_party_mid_name, 
	ext_lit_claim_party_name_prfx, 
	ext_lit_claim_party_name_sfx, 
	ext_lit_claim_party_addr, 
	ext_lit_claim_party_city, 
	ext_lit_claim_party_county, 
	ext_lit_claim_party_state, 
	ext_lit_claim_party_zip, 
	ext_lit_addr_type, 
	o_defd_claim_party_id, 
	o_defd_claim_party_ak_id, 
	o_defd_claim_party_full_Name, 
	o_defd_claim_party_first_name, 
	o_defd_claim_party_last_name, 
	o_defd_claim_party_mid_name, 
	o_defd_claim_party_name_prfx, 
	o_defd_claim_party_name_sfx
	FROM Agg_Defendant_Data
	ORDER BY claim_case_ak_id ASC, eff_from_date ASC
),
JNR_Def_Plt AS (SELECT
	SRT1.claim_case_id, 
	SRT1.claim_case_ak_id, 
	SRT1.claim_case_key, 
	SRT1.claim_case_name, 
	SRT1.claim_case_num, 
	SRT1.suit_county, 
	SRT1.suit_state, 
	SRT1.trial_date, 
	SRT1.first_notice_law_suit_ind, 
	SRT1.declaratory_action_ind, 
	SRT1.suit_status_code, 
	SRT1.suit_status_code_desc, 
	SRT1.suit_denial_date, 
	SRT1.prim_litigation_handler_role_code, 
	SRT1.prim_litigation_handler_role_code_desc, 
	SRT1.suit_open_date, 
	SRT1.suit_close_date, 
	SRT1.suit_how_claim_closed, 
	SRT1.reins_reported_ind, 
	SRT1.commercl_umb_reserve, 
	SRT1.suit_pay_amt, 
	SRT1.arbitration_open_date, 
	SRT1.arbitration_close_date, 
	SRT1.demand_at_initial_litigation, 
	SRT1.crrnt_snpsht_flag, 
	SRT1.audit_id, 
	SRT1.eff_from_date, 
	SRT1.eff_to_date, 
	SRT1.created_date, 
	SRT1.modified_date, 
	SRT1.ext_lit_pk_id, 
	SRT1.ext_lit_ak_id, 
	SRT1.ext_lit_claim_party_full_name, 
	SRT1.ext_lit_claim_party_first_name, 
	SRT1.ext_lit_claim_party_last_name, 
	SRT1.ext_lit_claim_party_mid_name, 
	SRT1.ext_lit_claim_party_name_prfx, 
	SRT1.ext_lit_claim_party_name_sfx, 
	SRT1.ext_lit_claim_party_addr, 
	SRT1.ext_lit_claim_party_city, 
	SRT1.ext_lit_claim_party_county, 
	SRT1.ext_lit_claim_party_state, 
	SRT1.ext_lit_claim_party_zip, 
	SRT1.ext_lit_addr_type, 
	SRT1.o_defd_claim_party_id, 
	SRT1.o_defd_claim_party_ak_id, 
	SRT1.o_defd_claim_party_full_Name, 
	SRT1.o_defd_claim_party_first_name, 
	SRT1.o_defd_claim_party_last_name, 
	SRT1.o_defd_claim_party_mid_name, 
	SRT1.o_defd_claim_party_name_prfx, 
	SRT1.o_defd_claim_party_name_sfx, 
	SR2.claim_case_ak_id AS claim_case_ak_id1, 
	SR2.eff_from_date AS eff_from_date1, 
	SR2.o_plat_claim_party_id, 
	SR2.o_plat_claim_party_ak_id, 
	SR2.o_plat_claim_party_full_name, 
	SR2.o_plat_claim_party_first_name, 
	SR2.o_plat_claim_party_last_name, 
	SR2.o_plat_claim_party_mid_name, 
	SR2.o_plat_claim_party_name_prfx, 
	SR2.o_plat_claim_party_name_sfx
	FROM SRT1
	INNER JOIN SR2
	ON SR2.claim_case_ak_id = SRT1.claim_case_ak_id AND SR2.eff_from_date = SRT1.eff_from_date
),
LKP_Question_Center_IsInjuredPartyRepresented_daily AS (
	SELECT
	claim_case_ak_id,
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id,
	optn_text,
	IN_claim_case_ak_id
	FROM (
		Select 
		CPO.claim_case_ak_id as claim_case_ak_id,
		CO.claim_occurrence_ak_id as claim_occurrence_ak_id ,
		CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id ,
		rtrim(ltrim(CA.optn_text)) as optn_text
		From
		claim_party_occurrence CPO
		inner join claim_answer CA
		on CA.claim_party_occurrence_ak_id = CPO.claim_party_occurrence_ak_id
		inner join claim_occurrence CO 
		on CA.claim_occurrence_ak_id = CO.claim_occurrence_ak_id
		inner join question Q
		on Q.question_ak_id = CA.question_ak_id
		inner join application_context AC
		on Q.app_context_ak_id = AC.app_context_ak_id
		inner join [application] APP
		on AC.app_ak_id = APP.app_ak_id
		Where 
		APP.display_name = 'Claims Workers Compensation' AND
		AC.app_context_entity_name = 'Claimant.Litigation.Questions' AND
		Q.logical_name = 'WasInjuredWorkerRepresented' and
		RTRIM(CPO.claim_party_role_code) in ('CLMT', 'CMT') and
		CO.crrnt_snpsht_flag = 1 and
		CPO.crrnt_snpsht_flag = 1 and
		APP.crrnt_snpsht_flag=1 and
		AC.crrnt_snpsht_flag =1 and
		Q.crrnt_snpsht_flag = 1 and
		CA.crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_ak_id ORDER BY claim_case_ak_id DESC) = 1
),
EXP_validate_lookup_values AS (
	SELECT
	optn_text,
	-- *INF*: IIF(ISNULL(optn_text),'N/A',optn_text)
	-- 
	IFF(optn_text IS NULL, 'N/A', optn_text) AS OUT_InjuredWorkerRepresentedFlag
	FROM LKP_Question_Center_IsInjuredPartyRepresented_daily
),
LKP_Target AS (
	SELECT
	claim_case_dim_id,
	claim_case_id,
	edw_claim_case_pk_id
	FROM (
		SELECT 
			claim_case_dim_id,
			claim_case_id,
			edw_claim_case_pk_id
		FROM claim_case_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_claim_case_pk_id ORDER BY claim_case_dim_id) = 1
),
RTR_Insert_Update AS (
	SELECT
	LKP_Target.claim_case_dim_id,
	JNR_Def_Plt.claim_case_id,
	JNR_Def_Plt.claim_case_ak_id,
	JNR_Def_Plt.claim_case_key,
	JNR_Def_Plt.claim_case_name,
	JNR_Def_Plt.claim_case_num,
	JNR_Def_Plt.suit_county,
	JNR_Def_Plt.suit_state,
	JNR_Def_Plt.trial_date,
	JNR_Def_Plt.first_notice_law_suit_ind,
	JNR_Def_Plt.declaratory_action_ind,
	JNR_Def_Plt.suit_status_code,
	JNR_Def_Plt.suit_status_code_desc,
	JNR_Def_Plt.suit_denial_date,
	JNR_Def_Plt.prim_litigation_handler_role_code,
	JNR_Def_Plt.prim_litigation_handler_role_code_desc,
	JNR_Def_Plt.suit_open_date,
	JNR_Def_Plt.suit_close_date,
	JNR_Def_Plt.suit_how_claim_closed,
	JNR_Def_Plt.reins_reported_ind,
	JNR_Def_Plt.commercl_umb_reserve,
	JNR_Def_Plt.suit_pay_amt,
	JNR_Def_Plt.arbitration_open_date,
	JNR_Def_Plt.arbitration_close_date,
	JNR_Def_Plt.demand_at_initial_litigation,
	JNR_Def_Plt.crrnt_snpsht_flag,
	JNR_Def_Plt.audit_id,
	JNR_Def_Plt.eff_from_date,
	JNR_Def_Plt.eff_to_date,
	JNR_Def_Plt.created_date,
	JNR_Def_Plt.modified_date,
	JNR_Def_Plt.ext_lit_pk_id,
	JNR_Def_Plt.ext_lit_ak_id,
	JNR_Def_Plt.ext_lit_claim_party_full_name,
	JNR_Def_Plt.ext_lit_claim_party_first_name,
	JNR_Def_Plt.ext_lit_claim_party_last_name,
	JNR_Def_Plt.ext_lit_claim_party_mid_name,
	JNR_Def_Plt.ext_lit_claim_party_name_prfx,
	JNR_Def_Plt.ext_lit_claim_party_name_sfx,
	JNR_Def_Plt.ext_lit_claim_party_addr,
	JNR_Def_Plt.ext_lit_claim_party_city,
	JNR_Def_Plt.ext_lit_claim_party_county,
	JNR_Def_Plt.ext_lit_claim_party_state,
	JNR_Def_Plt.ext_lit_claim_party_zip,
	JNR_Def_Plt.ext_lit_addr_type,
	JNR_Def_Plt.o_defd_claim_party_id,
	JNR_Def_Plt.o_defd_claim_party_ak_id,
	JNR_Def_Plt.o_defd_claim_party_full_Name,
	JNR_Def_Plt.o_defd_claim_party_first_name,
	JNR_Def_Plt.o_defd_claim_party_last_name,
	JNR_Def_Plt.o_defd_claim_party_mid_name,
	JNR_Def_Plt.o_defd_claim_party_name_prfx,
	JNR_Def_Plt.o_defd_claim_party_name_sfx,
	JNR_Def_Plt.o_plat_claim_party_id,
	JNR_Def_Plt.o_plat_claim_party_ak_id,
	JNR_Def_Plt.o_plat_claim_party_full_name,
	JNR_Def_Plt.o_plat_claim_party_first_name,
	JNR_Def_Plt.o_plat_claim_party_last_name,
	JNR_Def_Plt.o_plat_claim_party_mid_name,
	JNR_Def_Plt.o_plat_claim_party_name_prfx,
	JNR_Def_Plt.o_plat_claim_party_name_sfx,
	JNR_Def_Plt.claim_case_ak_id1 AS claim_case_ak_id_22,
	JNR_Def_Plt.eff_from_date1 AS eff_from_date_22,
	EXP_validate_lookup_values.OUT_InjuredWorkerRepresentedFlag
	FROM EXP_validate_lookup_values
	 -- Manually join with JNR_Def_Plt
	LEFT JOIN LKP_Target
	ON LKP_Target.edw_claim_case_pk_id = JNR_Def_Plt.claim_case_id
),
RTR_Insert_Update_Insert AS (SELECT * FROM RTR_Insert_Update WHERE isnull(claim_case_dim_id)),
RTR_Insert_Update_Update AS (SELECT * FROM RTR_Insert_Update WHERE not isnull(claim_case_dim_id)),
claim_case_dim_insert AS (
	INSERT INTO claim_case_dim
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, created_date, modified_date, edw_claim_case_pk_id, edw_claim_case_ak_id, edw_claim_party_pk_id_ext_litigation_handler, edw_claim_party_pk_id_plaintiff, edw_claim_party_pk_id_defendant, edw_claim_party_ak_id_ext_litigation_handler, edw_claim_party_ak_id_plaintiff, edw_claim_party_ak_id_defendant, claim_case_key, claim_case_name, claim_case_num, suit_county, suit_state, trial_date, first_notice_law_suit_ind, declaratory_action_ind, suit_status_code, suit_status_code_descript, suit_denial_date, prim_litigation_handler_role_code, prim_litigation_role_code_descript, suit_open_date, suit_close_date, suit_how_claim_closed, reins_reported_ind, commercl_umb_reserve, suit_pay_amt, arbitration_open_date, arbitration_close_date, demand_at_initial_litigation, ext_litigation_handler_addr_type, ext_litigation_handler_zip, ext_litigation_handler_state, ext_litigation_handler_county, ext_litigation_handler_city, ext_litigation_handler_addr, ext_litigation_handler_full_name, ext_litigation_handler_first_name, ext_litigation_handler_last_name, ext_litigation_handler_mid_name, ext_litigation_handler_name_prfx, ext_litigation_handler_name_sfx, plaintiff_full_name, plaintiff_first_name, plaintiff_last_name, plaintiff_mid_name, plaintiff_name_prfx, plaintiff_name_sfx, defendant_full_name, defendant_first_name, defendant_last_name, defendant_mid_name, defendant_name_prfx, defendant_name_sfx, InjuredWorkerRepresentedFlag)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	claim_case_id AS EDW_CLAIM_CASE_PK_ID, 
	claim_case_ak_id AS EDW_CLAIM_CASE_AK_ID, 
	ext_lit_pk_id AS EDW_CLAIM_PARTY_PK_ID_EXT_LITIGATION_HANDLER, 
	o_plat_claim_party_id AS EDW_CLAIM_PARTY_PK_ID_PLAINTIFF, 
	o_defd_claim_party_id AS EDW_CLAIM_PARTY_PK_ID_DEFENDANT, 
	ext_lit_ak_id AS EDW_CLAIM_PARTY_AK_ID_EXT_LITIGATION_HANDLER, 
	o_plat_claim_party_ak_id AS EDW_CLAIM_PARTY_AK_ID_PLAINTIFF, 
	o_defd_claim_party_ak_id AS EDW_CLAIM_PARTY_AK_ID_DEFENDANT, 
	CLAIM_CASE_KEY, 
	CLAIM_CASE_NAME, 
	CLAIM_CASE_NUM, 
	SUIT_COUNTY, 
	SUIT_STATE, 
	TRIAL_DATE, 
	FIRST_NOTICE_LAW_SUIT_IND, 
	DECLARATORY_ACTION_IND, 
	SUIT_STATUS_CODE, 
	suit_status_code_desc AS SUIT_STATUS_CODE_DESCRIPT, 
	SUIT_DENIAL_DATE, 
	PRIM_LITIGATION_HANDLER_ROLE_CODE, 
	prim_litigation_handler_role_code_desc AS PRIM_LITIGATION_ROLE_CODE_DESCRIPT, 
	SUIT_OPEN_DATE, 
	SUIT_CLOSE_DATE, 
	SUIT_HOW_CLAIM_CLOSED, 
	REINS_REPORTED_IND, 
	COMMERCL_UMB_RESERVE, 
	SUIT_PAY_AMT, 
	ARBITRATION_OPEN_DATE, 
	ARBITRATION_CLOSE_DATE, 
	DEMAND_AT_INITIAL_LITIGATION, 
	ext_lit_addr_type AS EXT_LITIGATION_HANDLER_ADDR_TYPE, 
	ext_lit_claim_party_zip AS EXT_LITIGATION_HANDLER_ZIP, 
	ext_lit_claim_party_state AS EXT_LITIGATION_HANDLER_STATE, 
	ext_lit_claim_party_county AS EXT_LITIGATION_HANDLER_COUNTY, 
	ext_lit_claim_party_city AS EXT_LITIGATION_HANDLER_CITY, 
	ext_lit_claim_party_addr AS EXT_LITIGATION_HANDLER_ADDR, 
	ext_lit_claim_party_full_name AS EXT_LITIGATION_HANDLER_FULL_NAME, 
	ext_lit_claim_party_first_name AS EXT_LITIGATION_HANDLER_FIRST_NAME, 
	ext_lit_claim_party_last_name AS EXT_LITIGATION_HANDLER_LAST_NAME, 
	ext_lit_claim_party_mid_name AS EXT_LITIGATION_HANDLER_MID_NAME, 
	ext_lit_claim_party_name_prfx AS EXT_LITIGATION_HANDLER_NAME_PRFX, 
	ext_lit_claim_party_name_sfx AS EXT_LITIGATION_HANDLER_NAME_SFX, 
	o_plat_claim_party_full_name AS PLAINTIFF_FULL_NAME, 
	o_plat_claim_party_first_name AS PLAINTIFF_FIRST_NAME, 
	o_plat_claim_party_last_name AS PLAINTIFF_LAST_NAME, 
	o_plat_claim_party_mid_name AS PLAINTIFF_MID_NAME, 
	o_plat_claim_party_name_prfx AS PLAINTIFF_NAME_PRFX, 
	o_plat_claim_party_name_sfx AS PLAINTIFF_NAME_SFX, 
	o_defd_claim_party_full_Name AS DEFENDANT_FULL_NAME, 
	o_defd_claim_party_first_name AS DEFENDANT_FIRST_NAME, 
	o_defd_claim_party_last_name AS DEFENDANT_LAST_NAME, 
	o_defd_claim_party_mid_name AS DEFENDANT_MID_NAME, 
	o_defd_claim_party_name_prfx AS DEFENDANT_NAME_PRFX, 
	o_defd_claim_party_name_sfx AS DEFENDANT_NAME_SFX, 
	OUT_InjuredWorkerRepresentedFlag AS INJUREDWORKERREPRESENTEDFLAG
	FROM RTR_Insert_Update_Insert
),
UPD_Update AS (
	SELECT
	claim_case_dim_id AS claim_case_dim_id3, 
	claim_case_id, 
	claim_case_ak_id, 
	claim_case_key AS claim_case_key3, 
	claim_case_name AS claim_case_name3, 
	claim_case_num AS claim_case_num3, 
	suit_county AS suit_county3, 
	suit_state AS suit_state3, 
	trial_date AS trial_date3, 
	first_notice_law_suit_ind AS first_notice_law_suit_ind3, 
	declaratory_action_ind AS declaratory_action_ind3, 
	suit_status_code AS suit_status_code3, 
	suit_status_code_desc AS suit_status_code_desc3, 
	suit_denial_date AS suit_denial_date3, 
	prim_litigation_handler_role_code AS prim_litigation_handler_role_code3, 
	prim_litigation_handler_role_code_desc AS prim_litigation_handler_role_code_desc3, 
	suit_open_date AS suit_open_date3, 
	suit_close_date AS suit_close_date3, 
	suit_how_claim_closed AS suit_how_claim_closed3, 
	reins_reported_ind AS reins_reported_ind3, 
	commercl_umb_reserve AS commercl_umb_reserve3, 
	suit_pay_amt AS suit_pay_amt3, 
	arbitration_open_date AS arbitration_open_date3, 
	arbitration_close_date AS arbitration_close_date3, 
	demand_at_initial_litigation AS demand_at_initial_litigation3, 
	crrnt_snpsht_flag AS crrnt_snpsht_flag3, 
	audit_id AS audit_id3, 
	eff_from_date AS eff_from_date3, 
	eff_to_date AS eff_to_date3, 
	created_date AS created_date3, 
	modified_date AS modified_date3, 
	ext_lit_pk_id AS ext_lit_pk_id3, 
	ext_lit_ak_id AS ext_lit_ak_id3, 
	ext_lit_claim_party_full_name AS ext_lit_claim_party_full_name3, 
	ext_lit_claim_party_first_name AS ext_lit_claim_party_first_name3, 
	ext_lit_claim_party_last_name AS ext_lit_claim_party_last_name3, 
	ext_lit_claim_party_mid_name AS ext_lit_claim_party_mid_name3, 
	ext_lit_claim_party_name_prfx AS ext_lit_claim_party_name_prfx3, 
	ext_lit_claim_party_name_sfx AS ext_lit_claim_party_name_sfx3, 
	ext_lit_claim_party_addr AS ext_lit_claim_party_addr3, 
	ext_lit_claim_party_city AS ext_lit_claim_party_city3, 
	ext_lit_claim_party_county AS ext_lit_claim_party_county3, 
	ext_lit_claim_party_state AS ext_lit_claim_party_state3, 
	ext_lit_claim_party_zip AS ext_lit_claim_party_zip3, 
	ext_lit_addr_type AS ext_lit_addr_type3, 
	o_defd_claim_party_id AS o_defd_claim_party_id3, 
	o_defd_claim_party_ak_id AS o_defd_claim_party_ak_id3, 
	o_defd_claim_party_full_Name AS o_defd_claim_party_full_Name3, 
	o_defd_claim_party_first_name AS o_defd_claim_party_first_name3, 
	o_defd_claim_party_last_name AS o_defd_claim_party_last_name3, 
	o_defd_claim_party_mid_name AS o_defd_claim_party_mid_name3, 
	o_defd_claim_party_name_prfx AS o_defd_claim_party_name_prfx3, 
	o_defd_claim_party_name_sfx AS o_defd_claim_party_name_sfx3, 
	o_plat_claim_party_id AS o_plat_claim_party_id3, 
	o_plat_claim_party_ak_id AS o_plat_claim_party_ak_id3, 
	o_plat_claim_party_full_name AS o_plat_claim_party_full_name3, 
	o_plat_claim_party_first_name AS o_plat_claim_party_first_name3, 
	o_plat_claim_party_last_name AS o_plat_claim_party_last_name3, 
	o_plat_claim_party_mid_name AS o_plat_claim_party_mid_name3, 
	o_plat_claim_party_name_prfx AS o_plat_claim_party_name_prfx3, 
	o_plat_claim_party_name_sfx AS o_plat_claim_party_name_sfx3, 
	OUT_InjuredWorkerRepresentedFlag AS OUT_InjuredWorkerRepresentedFlag3
	FROM RTR_Insert_Update_Update
),
claim_case_dim_update AS (
	MERGE INTO claim_case_dim AS T
	USING UPD_Update AS S
	ON T.claim_case_dim_id = S.claim_case_dim_id3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag3, T.audit_id = S.audit_id3, T.eff_from_date = S.eff_from_date3, T.eff_to_date = S.eff_to_date3, T.created_date = S.created_date3, T.modified_date = S.modified_date3, T.edw_claim_case_pk_id = S.claim_case_id, T.edw_claim_case_ak_id = S.claim_case_ak_id, T.edw_claim_party_pk_id_ext_litigation_handler = S.ext_lit_pk_id3, T.edw_claim_party_pk_id_plaintiff = S.o_plat_claim_party_id3, T.edw_claim_party_pk_id_defendant = S.o_defd_claim_party_id3, T.edw_claim_party_ak_id_ext_litigation_handler = S.ext_lit_ak_id3, T.edw_claim_party_ak_id_plaintiff = S.o_plat_claim_party_ak_id3, T.edw_claim_party_ak_id_defendant = S.o_defd_claim_party_ak_id3, T.claim_case_key = S.claim_case_key3, T.claim_case_name = S.claim_case_name3, T.claim_case_num = S.claim_case_num3, T.suit_county = S.suit_county3, T.suit_state = S.suit_state3, T.trial_date = S.trial_date3, T.first_notice_law_suit_ind = S.first_notice_law_suit_ind3, T.declaratory_action_ind = S.declaratory_action_ind3, T.suit_status_code = S.suit_status_code3, T.suit_status_code_descript = S.suit_status_code_desc3, T.suit_denial_date = S.suit_denial_date3, T.prim_litigation_handler_role_code = S.prim_litigation_handler_role_code3, T.prim_litigation_role_code_descript = S.prim_litigation_handler_role_code_desc3, T.suit_open_date = S.suit_open_date3, T.suit_close_date = S.suit_close_date3, T.suit_how_claim_closed = S.suit_how_claim_closed3, T.reins_reported_ind = S.reins_reported_ind3, T.commercl_umb_reserve = S.commercl_umb_reserve3, T.suit_pay_amt = S.suit_pay_amt3, T.arbitration_open_date = S.arbitration_open_date3, T.arbitration_close_date = S.arbitration_close_date3, T.demand_at_initial_litigation = S.demand_at_initial_litigation3, T.ext_litigation_handler_addr_type = S.ext_lit_addr_type3, T.ext_litigation_handler_zip = S.ext_lit_claim_party_zip3, T.ext_litigation_handler_state = S.ext_lit_claim_party_state3, T.ext_litigation_handler_county = S.ext_lit_claim_party_county3, T.ext_litigation_handler_city = S.ext_lit_claim_party_city3, T.ext_litigation_handler_addr = S.ext_lit_claim_party_addr3, T.ext_litigation_handler_full_name = S.ext_lit_claim_party_full_name3, T.ext_litigation_handler_first_name = S.ext_lit_claim_party_first_name3, T.ext_litigation_handler_last_name = S.ext_lit_claim_party_last_name3, T.ext_litigation_handler_mid_name = S.ext_lit_claim_party_mid_name3, T.ext_litigation_handler_name_prfx = S.ext_lit_claim_party_name_prfx3, T.ext_litigation_handler_name_sfx = S.ext_lit_claim_party_name_sfx3, T.plaintiff_full_name = S.o_plat_claim_party_full_name3, T.plaintiff_first_name = S.o_plat_claim_party_first_name3, T.plaintiff_last_name = S.o_plat_claim_party_last_name3, T.plaintiff_mid_name = S.o_plat_claim_party_mid_name3, T.plaintiff_name_prfx = S.o_plat_claim_party_name_prfx3, T.plaintiff_name_sfx = S.o_plat_claim_party_name_sfx3, T.defendant_full_name = S.o_defd_claim_party_full_Name3, T.defendant_first_name = S.o_defd_claim_party_first_name3, T.defendant_last_name = S.o_defd_claim_party_last_name3, T.defendant_mid_name = S.o_defd_claim_party_mid_name3, T.defendant_name_prfx = S.o_defd_claim_party_name_prfx3, T.defendant_name_sfx = S.o_defd_claim_party_name_sfx3, T.InjuredWorkerRepresentedFlag = S.OUT_InjuredWorkerRepresentedFlag3
),
SQ_claim_case_dim AS (
	SELECT 
	  a.claim_case_dim_id
	, a.eff_from_date
	, a.eff_to_date
	, a.edw_claim_case_ak_id
	FROM
	 @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIM_CASE_DIM A
	WHERE 
	EXISTS
	(
	SELECT  1 FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CLAIM_CASE_DIM B
	WHERE CRRNT_SNPSHT_FLAG = 1 AND 
	A.EDW_CLAIM_CASE_AK_ID =B.EDW_CLAIM_CASE_AK_ID
	GROUP BY B.EDW_CLAIM_CASE_AK_ID
	HAVING COUNT(*) > 1
	)
	ORDER BY A.EDW_CLAIM_CASE_AK_ID ,A.EFF_FROM_DATE DESC
),
EXP_Source AS (
	SELECT
	claim_case_dim_id,
	edw_claim_case_ak_id,
	eff_from_date,
	eff_to_date AS orig_eff_to_date,
	-- *INF*: DECODE(TRUE,
	-- 	edw_claim_case_ak_id=v_PREV_ROW_edw_claim_case_ak_id 
	--  	,ADD_TO_DATE(v_PREV_ROW_eff_from_date,'SS',-1)
	--        ,orig_eff_to_date)
	-- 
	-- 
	-- 
	DECODE(
	    TRUE,
	    edw_claim_case_ak_id = v_PREV_ROW_edw_claim_case_ak_id, DATEADD(SECOND,- 1,v_PREV_ROW_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS o_eff_to_date,
	eff_from_date AS v_PREV_ROW_eff_from_date,
	edw_claim_case_ak_id AS v_PREV_ROW_edw_claim_case_ak_id,
	sysdate AS modified_date,
	0 AS crrnt_snpsht_flag
	FROM SQ_claim_case_dim
),
FLT_Source_Rows AS (
	SELECT
	claim_case_dim_id, 
	orig_eff_to_date, 
	o_eff_to_date AS eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM EXP_Source
	WHERE orig_eff_to_date <> eff_to_date
),
Upd_Update_Eff_Dates AS (
	SELECT
	claim_case_dim_id, 
	eff_to_date, 
	modified_date, 
	crrnt_snpsht_flag
	FROM FLT_Source_Rows
),
claim_case_dim_update_dates AS (
	MERGE INTO claim_case_dim AS T
	USING Upd_Update_Eff_Dates AS S
	ON T.claim_case_dim_id = S.claim_case_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),
SQ_claim_case_dim_UPDATE_Question_Center_Fields AS (
	SELECT
		claim_case_dim_id,
		crrnt_snpsht_flag,
		audit_id,
		eff_from_date,
		eff_to_date,
		created_date,
		modified_date,
		edw_claim_case_pk_id,
		edw_claim_case_ak_id,
		edw_claim_party_pk_id_ext_litigation_handler,
		edw_claim_party_pk_id_plaintiff,
		edw_claim_party_pk_id_defendant,
		edw_claim_party_ak_id_ext_litigation_handler,
		edw_claim_party_ak_id_plaintiff,
		edw_claim_party_ak_id_defendant,
		claim_case_key,
		claim_case_name,
		claim_case_num,
		suit_county,
		suit_state,
		trial_date,
		first_notice_law_suit_ind,
		declaratory_action_ind,
		suit_status_code,
		suit_status_code_descript,
		suit_denial_date,
		prim_litigation_handler_role_code,
		prim_litigation_role_code_descript,
		suit_open_date,
		suit_close_date,
		suit_how_claim_closed,
		reins_reported_ind,
		commercl_umb_reserve,
		suit_pay_amt,
		arbitration_open_date,
		arbitration_close_date,
		demand_at_initial_litigation,
		ext_litigation_handler_addr_type,
		ext_litigation_handler_zip,
		ext_litigation_handler_state,
		ext_litigation_handler_county,
		ext_litigation_handler_city,
		ext_litigation_handler_addr,
		ext_litigation_handler_full_name,
		ext_litigation_handler_first_name,
		ext_litigation_handler_last_name,
		ext_litigation_handler_mid_name,
		ext_litigation_handler_name_prfx,
		ext_litigation_handler_name_sfx,
		plaintiff_full_name,
		plaintiff_first_name,
		plaintiff_last_name,
		plaintiff_mid_name,
		plaintiff_name_prfx,
		plaintiff_name_sfx,
		defendant_full_name,
		defendant_first_name,
		defendant_last_name,
		defendant_mid_name,
		defendant_name_prfx,
		defendant_name_sfx,
		InjuredWorkerRepresentedFlag
	FROM claim_case_dim_UPDATE_Question_Center_Fields
	WHERE claim_case_dim.crrnt_snpsht_flag=1
),
EXP_Input_for_QuestionCenter_InjuredPartyRep_Updates AS (
	SELECT
	claim_case_dim_id,
	edw_claim_case_ak_id
	FROM SQ_claim_case_dim_UPDATE_Question_Center_Fields
),
LKP_Question_Center_IsInjuredPartyRepresented AS (
	SELECT
	claim_case_ak_id,
	claim_occurrence_ak_id,
	claim_party_occurrence_ak_id,
	optn_text,
	IN_claim_case_ak_id,
	IN_claim_case_dim_id
	FROM (
		Select 
		CPO.claim_case_ak_id as claim_case_ak_id,
		CO.claim_occurrence_ak_id as claim_occurrence_ak_id ,
		CPO.claim_party_occurrence_ak_id as claim_party_occurrence_ak_id ,
		rtrim(ltrim(CA.optn_text)) as optn_text
		From
		claim_party_occurrence CPO
		inner join claim_answer CA
		on CA.claim_party_occurrence_ak_id = CPO.claim_party_occurrence_ak_id
		inner join claim_occurrence CO 
		on CA.claim_occurrence_ak_id = CO.claim_occurrence_ak_id
		inner join question Q
		on Q.question_ak_id = CA.question_ak_id
		inner join application_context AC
		on Q.app_context_ak_id = AC.app_context_ak_id
		inner join [application] APP
		on AC.app_ak_id = APP.app_ak_id
		Where 
		APP.display_name = 'Claims Workers Compensation' AND
		AC.app_context_entity_name = 'Claimant.Litigation.Questions' AND
		Q.logical_name = 'WasInjuredWorkerRepresented' and
		RTRIM(CPO.claim_party_role_code) in ('CLMT', 'CMT') and
		CO.crrnt_snpsht_flag = 1 and
		CPO.crrnt_snpsht_flag = 1 and
		APP.crrnt_snpsht_flag=1 and
		AC.crrnt_snpsht_flag =1 and
		Q.crrnt_snpsht_flag = 1 and
		CA.crrnt_snpsht_flag =1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claim_case_ak_id ORDER BY claim_case_ak_id DESC) = 1
),
FILTRANS AS (
	SELECT
	IN_claim_case_dim_id AS claim_case_dim_id, 
	optn_text
	FROM LKP_Question_Center_IsInjuredPartyRepresented
	WHERE NOT ISNULL(optn_text)
),
UPD_InjuredWorkerRep_update AS (
	SELECT
	claim_case_dim_id, 
	optn_text
	FROM FILTRANS
),
claim_case_dim_UPDATE_Question_Center_InjWorkerRep AS (
	MERGE INTO claim_case_dim AS T
	USING UPD_InjuredWorkerRep_update AS S
	ON T.claim_case_dim_id = S.claim_case_dim_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.InjuredWorkerRepresentedFlag = S.optn_text
),