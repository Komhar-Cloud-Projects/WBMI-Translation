WITH
SQ_pif_02_stage AS (
	SELECT 
	a.pif_symbol
	, a.pif_policy_number
	, a.pif_module
	, CAST(CAST(LTRIM(RTRIM(a.pif_risk_state_prov)) AS INT) AS VARCHAR(2)) AS pif_risk_state_prov
	, a.pif_customer_number
	, a.pif_zip_5_digit_code
	, a.pif_address_line_1
	, a.pif_insured_name_cont
	, a.pif_address_line_2_b
	, a.pif_address_line_3
	, a.pif_address_line_4
	, a.pif_wbc_county 
	FROM
	 @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_02_stage a
),
LKP_sup_state AS (
	SELECT
	state_code,
	state_abbrev
	FROM (
		SELECT 
			state_code,
			state_abbrev
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_state
		WHERE crrnt_snpsht_flag=1 and source_sys_id = 'EXCEED'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY state_abbrev ORDER BY state_code) = 1
),
EXP_values AS (
	SELECT
	SQ_pif_02_stage.pif_symbol,
	SQ_pif_02_stage.pif_policy_number,
	SQ_pif_02_stage.pif_module,
	-- *INF*: ltrim(rtrim(pif_symbol)) || ltrim(rtrim(pif_policy_number)) || ltrim(rtrim(pif_module))
	ltrim(rtrim(pif_symbol)) || ltrim(rtrim(pif_policy_number)) || ltrim(rtrim(pif_module)) AS v_contract_key,
	-- *INF*: ltrim(rtrim(v_contract_key))
	ltrim(rtrim(v_contract_key)) AS contract_key,
	-- *INF*: LTRIM(RTRIM('MAILING'))
	LTRIM(RTRIM('MAILING')) AS addr_type,
	SQ_pif_02_stage.pif_customer_number AS in_pif_customer_number,
	SQ_pif_02_stage.pif_zip_5_digit_code AS in_pif_zip_5_digit_code,
	-- *INF*: iif(isnull(in_pif_customer_number) or IS_SPACES(in_pif_customer_number) or LENGTH(in_pif_customer_number)=0,'0000000000',ltrim(rtrim(in_pif_customer_number)))
	IFF(
	    in_pif_customer_number IS NULL
	    or LENGTH(in_pif_customer_number)>0
	    and TRIM(in_pif_customer_number)=''
	    or LENGTH(in_pif_customer_number) = 0,
	    '0000000000',
	    ltrim(rtrim(in_pif_customer_number))
	) AS v_customer_num,
	v_customer_num AS customer_number,
	'INS' AS cust_role_code,
	SQ_pif_02_stage.pif_address_line_1 AS in_pif_address_line_1,
	-- *INF*: ltrim(rtrim(in_pif_address_line_1))
	ltrim(rtrim(in_pif_address_line_1)) AS v_pif_address_line_1,
	SQ_pif_02_stage.pif_insured_name_cont AS in_pif_insured_name_cont,
	SQ_pif_02_stage.pif_address_line_2_b AS in_pif_address_line_2_b,
	-- *INF*: ltrim(rtrim(in_pif_address_line_2_b))
	ltrim(rtrim(in_pif_address_line_2_b)) AS v_pif_address_line_2_b,
	SQ_pif_02_stage.pif_address_line_3 AS in_pif_address_line_3,
	-- *INF*: ltrim(rtrim(in_pif_address_line_3))
	ltrim(rtrim(in_pif_address_line_3)) AS v_pif_address_line_3,
	SQ_pif_02_stage.pif_address_line_4 AS in_pif_address_line_4,
	SQ_pif_02_stage.pif_wbc_county AS in_pif_wbc_county,
	-- *INF*: ltrim(rtrim(in_pif_address_line_4))
	ltrim(rtrim(in_pif_address_line_4)) AS v_pif_address_line_4,
	-- *INF*: ltrim(in_pif_insured_name_cont || v_pif_address_line_2_b)
	ltrim(in_pif_insured_name_cont || v_pif_address_line_2_b) AS v_pif_insured_cont_add_line_2,
	-- *INF*: iif(iif(IS_SPACES(v_pif_address_line_4) or isnull(v_pif_address_line_4) or LENGTH(v_pif_address_line_4)=0,
	-- 1,0)=1,
	-- (iif(decode(SUBSTR(v_pif_address_line_2_b,(LENGTH(v_pif_address_line_2_b)-3),4),
	-- ' LLC',1,
	-- ' DBA',1,
	-- ' BA;',1,
	-- ' BA,',1,
	-- ' SOC',1,
	-- 'PLAN',1,
	-- 'CORP',1,
	-- 0)=1,'N/A',v_pif_insured_cont_add_line_2)),
	-- iif(iif(IN(SUBSTR(v_pif_address_line_2_b,1,1),'1','2','3','4','5','6','7','8','9','0',0)=1,1,iif(decode(substr(v_pif_insured_cont_add_line_2,1,4),
	-- 'POST',1,
	-- 'PO B',1,0)=1,1,0))=0,
	-- iif(decode(SUBSTR(v_pif_address_line_2_b,(LENGTH(v_pif_address_line_2_b)-2),3) ,
	-- ' ST',1,
	-- ' RD',1,
	-- ' PL',1,0)=1,v_pif_insured_cont_add_line_2,v_pif_address_line_3),
	-- iif(iif(decode(SUBSTR(v_pif_address_line_2_b,(LENGTH(v_pif_address_line_2_b)-3),4),
	-- ' LLC',1,
	-- ' DBA',1,
	-- ' BA;',1,
	-- ' BA,',1,
	-- ' SOC',1,
	-- 'PLAN',1,
	-- 'CORP',1,
	-- 0)=1,1,iif(IIF(NOT (ISNULL(v_pif_address_line_2_b)) OR NOT (IS_SPACES(v_pif_address_line_2_b)), instr(v_pif_address_line_2_b,'TRUST'),0) != 0,1,iif(IIF(NOT (ISNULL(v_pif_address_line_1)) OR NOT (IS_SPACES(v_pif_address_line_1)), instr(v_pif_address_line_1,'TRUST'),0) != 0,1,0))) =1,v_pif_address_line_3,v_pif_insured_cont_add_line_2)))
	IFF(
	    IFF(
	        LENGTH(v_pif_address_line_4)>0
	        and TRIM(v_pif_address_line_4)=''
	        or v_pif_address_line_4 IS NULL
	        or LENGTH(v_pif_address_line_4) = 0,
	        1,
	        0
	    ) = 1,
	    (
	        IFF(
	            decode(
	                SUBSTR(v_pif_address_line_2_b, (LENGTH(v_pif_address_line_2_b) - 3), 4),
	                ' LLC', 1,
	                ' DBA', 1,
	                ' BA;', 1,
	                ' BA,', 1,
	                ' SOC', 1,
	                'PLAN', 1,
	                'CORP', 1,
	                0
	            ) = 1,
	            'N/A',
	            v_pif_insured_cont_add_line_2
	        )),
	    IFF(
	        IFF(
	            SUBSTR(v_pif_address_line_2_b, 1, 1) IN ('1','2','3','4','5','6','7','8','9','0',0) = 1,
	            1,
	            IFF(
	                decode(
	                    substr(v_pif_insured_cont_add_line_2, 1, 4),
	                    'POST', 1,
	                    'PO B', 1,
	                    0
	                ) = 1,
	                1,
	                0
	            )
	        ) = 0,
	        IFF(
	            decode(
	                SUBSTR(v_pif_address_line_2_b, (LENGTH(v_pif_address_line_2_b) - 2), 3),
	                ' ST', 1,
	                ' RD', 1,
	                ' PL', 1,
	                0
	            ) = 1,
	            v_pif_insured_cont_add_line_2,
	            v_pif_address_line_3
	        ),
	        IFF(
	            IFF(
	                decode(
	                    SUBSTR(v_pif_address_line_2_b, (LENGTH(v_pif_address_line_2_b) - 3), 4),
	                    ' LLC', 1,
	                    ' DBA', 1,
	                    ' BA;', 1,
	                    ' BA,', 1,
	                    ' SOC', 1,
	                    'PLAN', 1,
	                    'CORP', 1,
	                    0
	                ) = 1,
	                1,
	                IFF(
	                    IFF(
	                        NOT (v_pif_address_line_2_b IS NULL)
	                        or NOT (LENGTH(v_pif_address_line_2_b)>0
	                        and TRIM(v_pif_address_line_2_b)=''),
	                        REGEXP_INSTR(v_pif_address_line_2_b, 'TRUST'),
	                        0
	                    ) != 0,
	                    1,
	                    IFF(
	                        IFF(
	                            NOT (v_pif_address_line_1 IS NULL)
	                            or NOT (LENGTH(v_pif_address_line_1)>0
	                            and TRIM(v_pif_address_line_1)=''),
	                            REGEXP_INSTR(v_pif_address_line_1, 'TRUST'),
	                            0
	                        ) != 0,
	                        1,
	                        0
	                    )
	                )
	            ) = 1,
	            v_pif_address_line_3,
	            v_pif_insured_cont_add_line_2
	        )
	    )
	) AS v_addr_line_1,
	-- *INF*: iif(iif(IS_SPACES(v_pif_address_line_4) or isnull(v_pif_address_line_4) or LENGTH(v_pif_address_line_4)=0,
	-- 1,0)=1,
	-- (iif(decode(SUBSTR(v_pif_address_line_2_b,(LENGTH(v_pif_address_line_2_b)-3),4),
	-- ' LLC',1,
	-- ' DBA',1,
	-- ' BA;',1,
	-- ' BA,',1,
	-- ' SOC',1,
	-- 'PLAN',1,
	-- 'CORP',1,
	-- 0)=1,'N/A',v_pif_insured_cont_add_line_2)),
	-- iif(iif(IN(SUBSTR(v_pif_address_line_2_b,1,1),'1','2','3','4','5','6','7','8','9','0',0)=1,1,iif(decode(substr(v_pif_insured_cont_add_line_2,1,4),
	-- 'POST',1,
	-- 'PO B',1,0)=1,1,0))=0,
	-- iif(decode(SUBSTR(v_pif_address_line_2_b,(LENGTH(v_pif_address_line_2_b)-2),3) ,
	-- ' ST',1,
	-- ' RD',1,
	-- ' PL',1,0)=1,v_pif_insured_cont_add_line_2,v_pif_address_line_3),
	-- iif(iif(decode(SUBSTR(v_pif_address_line_2_b,(LENGTH(v_pif_address_line_2_b)-3),4),
	-- ' LLC',1,
	-- ' DBA',1,
	-- ' BA;',1,
	-- ' BA,',1,
	-- ' SOC',1,
	-- 'PLAN',1,
	-- 'CORP',1,
	-- 0)=1,1,iif(IIF(NOT (ISNULL(v_pif_address_line_2_b)) OR NOT (IS_SPACES(v_pif_address_line_2_b)), instr(v_pif_address_line_2_b,'TRUST'),0) != 0,1,iif(IIF(NOT (ISNULL(v_pif_address_line_1)) OR NOT (IS_SPACES(v_pif_address_line_1)), instr(v_pif_address_line_1,'TRUST'),0) != 0,1,0))) =1,v_pif_address_line_3,v_pif_insured_cont_add_line_2)))
	IFF(
	    IFF(
	        LENGTH(v_pif_address_line_4)>0
	        and TRIM(v_pif_address_line_4)=''
	        or v_pif_address_line_4 IS NULL
	        or LENGTH(v_pif_address_line_4) = 0,
	        1,
	        0
	    ) = 1,
	    (
	        IFF(
	            decode(
	                SUBSTR(v_pif_address_line_2_b, (LENGTH(v_pif_address_line_2_b) - 3), 4),
	                ' LLC', 1,
	                ' DBA', 1,
	                ' BA;', 1,
	                ' BA,', 1,
	                ' SOC', 1,
	                'PLAN', 1,
	                'CORP', 1,
	                0
	            ) = 1,
	            'N/A',
	            v_pif_insured_cont_add_line_2
	        )),
	    IFF(
	        IFF(
	            SUBSTR(v_pif_address_line_2_b, 1, 1) IN ('1','2','3','4','5','6','7','8','9','0',0) = 1,
	            1,
	            IFF(
	                decode(
	                    substr(v_pif_insured_cont_add_line_2, 1, 4),
	                    'POST', 1,
	                    'PO B', 1,
	                    0
	                ) = 1,
	                1,
	                0
	            )
	        ) = 0,
	        IFF(
	            decode(
	                SUBSTR(v_pif_address_line_2_b, (LENGTH(v_pif_address_line_2_b) - 2), 3),
	                ' ST', 1,
	                ' RD', 1,
	                ' PL', 1,
	                0
	            ) = 1,
	            v_pif_insured_cont_add_line_2,
	            v_pif_address_line_3
	        ),
	        IFF(
	            IFF(
	                decode(
	                    SUBSTR(v_pif_address_line_2_b, (LENGTH(v_pif_address_line_2_b) - 3), 4),
	                    ' LLC', 1,
	                    ' DBA', 1,
	                    ' BA;', 1,
	                    ' BA,', 1,
	                    ' SOC', 1,
	                    'PLAN', 1,
	                    'CORP', 1,
	                    0
	                ) = 1,
	                1,
	                IFF(
	                    IFF(
	                        NOT (v_pif_address_line_2_b IS NULL)
	                        or NOT (LENGTH(v_pif_address_line_2_b)>0
	                        and TRIM(v_pif_address_line_2_b)=''),
	                        REGEXP_INSTR(v_pif_address_line_2_b, 'TRUST'),
	                        0
	                    ) != 0,
	                    1,
	                    IFF(
	                        IFF(
	                            NOT (v_pif_address_line_1 IS NULL)
	                            or NOT (LENGTH(v_pif_address_line_1)>0
	                            and TRIM(v_pif_address_line_1)=''),
	                            REGEXP_INSTR(v_pif_address_line_1, 'TRUST'),
	                            0
	                        ) != 0,
	                        1,
	                        0
	                    )
	                )
	            ) = 1,
	            v_pif_address_line_3,
	            v_pif_insured_cont_add_line_2
	        )
	    )
	) AS v_address_line_1,
	-- *INF*: iif(isnull(v_address_line_1) or IS_SPACES(v_address_line_1) or LENGTH(v_address_line_1)=0,'N/A',LTRIM(RTRIM(v_address_line_1)))
	IFF(
	    v_address_line_1 IS NULL
	    or LENGTH(v_address_line_1)>0
	    and TRIM(v_address_line_1)=''
	    or LENGTH(v_address_line_1) = 0,
	    'N/A',
	    LTRIM(RTRIM(v_address_line_1))
	) AS addr_line_1,
	-- *INF*: iif(iif(IS_SPACES(v_pif_address_line_4) or isnull(v_pif_address_line_4) or LENGTH(v_pif_address_line_4)=0,
	-- 1,0)=1,'N/A',
	-- iif(iif(IN(SUBSTR(v_pif_address_line_2_b,1,1),'1','2','3','4','5','6','7','8','9','0',0)=1,1,iif(decode(substr(v_pif_insured_cont_add_line_2,1,4),
	-- 'POST',1,
	-- 'PO B',1,0)=1,1,0))=0,
	-- iif(decode(SUBSTR(v_pif_address_line_2_b,(LENGTH(v_pif_address_line_2_b)-2),3) ,
	-- ' ST',1,
	-- ' RD',1,
	-- ' PL',1,0)=1,v_pif_address_line_3,'N/A'),
	-- iif(iif(decode(SUBSTR(v_pif_address_line_2_b,(LENGTH(v_pif_address_line_2_b)-3),4),
	-- ' LLC',1,
	-- ' DBA',1,
	-- ' BA;',1,
	-- ' BA,',1,
	-- ' SOC',1,
	-- 'PLAN',1,
	-- 'CORP',1,
	-- 0)=1,1,iif(IIF(NOT (ISNULL(v_pif_address_line_2_b)) OR NOT (IS_SPACES(v_pif_address_line_2_b)), instr(v_pif_address_line_2_b,'TRUST'),0) != 0,1,iif(IIF(NOT (ISNULL(v_pif_address_line_1)) OR NOT (IS_SPACES(v_pif_address_line_1)), instr(v_pif_address_line_1,'TRUST'),0) != 0,1,0))) =1,'N/A',v_pif_address_line_3)))
	IFF(
	    IFF(
	        LENGTH(v_pif_address_line_4)>0
	        and TRIM(v_pif_address_line_4)=''
	        or v_pif_address_line_4 IS NULL
	        or LENGTH(v_pif_address_line_4) = 0,
	        1,
	        0
	    ) = 1,
	    'N/A',
	    IFF(
	        IFF(
	            SUBSTR(v_pif_address_line_2_b, 1, 1) IN ('1','2','3','4','5','6','7','8','9','0',0) = 1,
	            1,
	            IFF(
	                decode(
	                    substr(v_pif_insured_cont_add_line_2, 1, 4),
	                    'POST', 1,
	                    'PO B', 1,
	                    0
	                ) = 1,
	                1,
	                0
	            )
	        ) = 0,
	        IFF(
	            decode(
	                SUBSTR(v_pif_address_line_2_b, (LENGTH(v_pif_address_line_2_b) - 2), 3),
	                ' ST', 1,
	                ' RD', 1,
	                ' PL', 1,
	                0
	            ) = 1,
	            v_pif_address_line_3,
	            'N/A'
	        ),
	        IFF(
	            IFF(
	                decode(
	                    SUBSTR(v_pif_address_line_2_b, (LENGTH(v_pif_address_line_2_b) - 3), 4),
	                    ' LLC', 1,
	                    ' DBA', 1,
	                    ' BA;', 1,
	                    ' BA,', 1,
	                    ' SOC', 1,
	                    'PLAN', 1,
	                    'CORP', 1,
	                    0
	                ) = 1,
	                1,
	                IFF(
	                    IFF(
	                        NOT (v_pif_address_line_2_b IS NULL)
	                        or NOT (LENGTH(v_pif_address_line_2_b)>0
	                        and TRIM(v_pif_address_line_2_b)=''),
	                        REGEXP_INSTR(v_pif_address_line_2_b, 'TRUST'),
	                        0
	                    ) != 0,
	                    1,
	                    IFF(
	                        IFF(
	                            NOT (v_pif_address_line_1 IS NULL)
	                            or NOT (LENGTH(v_pif_address_line_1)>0
	                            and TRIM(v_pif_address_line_1)=''),
	                            REGEXP_INSTR(v_pif_address_line_1, 'TRUST'),
	                            0
	                        ) != 0,
	                        1,
	                        0
	                    )
	                )
	            ) = 1,
	            'N/A',
	            v_pif_address_line_3
	        )
	    )
	) AS v_addr_line_2,
	-- *INF*: iif(isnull(v_addr_line_2) or IS_SPACES(v_addr_line_2) or LENGTH(v_addr_line_2)=0,'N/A',LTRIM(RTRIM(v_addr_line_2)))
	IFF(
	    v_addr_line_2 IS NULL
	    or LENGTH(v_addr_line_2)>0
	    and TRIM(v_addr_line_2)=''
	    or LENGTH(v_addr_line_2) = 0,
	    'N/A',
	    LTRIM(RTRIM(v_addr_line_2))
	) AS v_address_line_2,
	-- *INF*: v_address_line_2
	-- 
	-- --iif(isnull(v_addr_line_2) or IS_SPACES(v_addr_line_2) or LENGTH(v_addr_line_2)=0,'N/A',LTRIM(RTRIM(v_addr_line_2)))
	v_address_line_2 AS addr_line_2,
	'N/A' AS addr_line_3,
	-- *INF*: iif(iif(IS_SPACES(v_pif_address_line_4) or isnull(v_pif_address_line_4) or LENGTH(v_pif_address_line_4)=0,
	-- 1,0)=1,ltrim(rtrim(SUBSTR(v_pif_address_line_3,1,INSTR(v_pif_address_line_3,',')-1))),ltrim(rtrim(SUBSTR(v_pif_address_line_4,1,INSTR(v_pif_address_line_4,',')-1))))
	-- 
	-- 
	-- 
	-- --iif(iif(IS_SPACES(v_pif_address_line_4) or isnull(v_pif_address_line_4) or LENGTH(v_pif_address_line_4)=0,
	-- --1,0)=1,ltrim(rtrim(REPLACECHR( 1,SUBSTR(v_pif_address_line_3,1,INSTR(v_pif_address_line_3,',')),',',''))),
	-- --ltrim(rtrim--(REPLACECHR( 1,SUBSTR(v_pif_address_line_4,1,INSTR(v_pif_address_line_4,',')),',',''))))
	IFF(
	    IFF(
	        LENGTH(v_pif_address_line_4)>0
	        and TRIM(v_pif_address_line_4)=''
	        or v_pif_address_line_4 IS NULL
	        or LENGTH(v_pif_address_line_4) = 0,
	        1,
	        0
	    ) = 1,
	    ltrim(rtrim(SUBSTR(v_pif_address_line_3, 1, REGEXP_INSTR(v_pif_address_line_3, ',') - 1))),
	    ltrim(rtrim(SUBSTR(v_pif_address_line_4, 1, REGEXP_INSTR(v_pif_address_line_4, ',') - 1)))
	) AS v_city,
	-- *INF*: iif(isnull(v_city) or IS_SPACES(v_city) or LENGTH(v_city)=0,'N/A',ltrim(rtrim(v_city)))
	IFF(
	    v_city IS NULL or LENGTH(v_city)>0 AND TRIM(v_city)='' or LENGTH(v_city) = 0, 'N/A',
	    ltrim(rtrim(v_city))
	) AS v_city_1,
	-- *INF*: iif(iif(IS_SPACES(v_pif_address_line_4) or isnull(v_pif_address_line_4) or LENGTH(v_pif_address_line_4)=0,
	-- 1,0)=1,ltrim(rtrim(SUBSTR(v_pif_address_line_3,INSTR(v_pif_address_line_3,',',-1)+1))),ltrim(rtrim(SUBSTR(v_pif_address_line_4,INSTR(v_pif_address_line_4,',',-1)+1))))
	-- 
	-- 
	-- --iif(iif(IS_SPACES(v_pif_address_line_4) or isnull(v_pif_address_line_4) or LENGTH(v_pif_address_line_4)=0,
	-- --1,0)=1,ltrim(rtrim(SUBSTR(v_pif_address_line_3,INSTR(v_pif_address_line_3,',')+1))),ltrim(rtrim(SUBSTR--(v_pif_address_line_4,INSTR(v_pif_address_line_4,',')+1))))
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- --iif(iif(IS_SPACES(v_pif_address_line_4) or isnull(v_pif_address_line_4) or LENGTH(v_pif_address_line_4)=0,--
	-- --1,0)=1,ltrim(rtrim(REPLACECHR(1,SUBSTR(v_pif_address_line_3,INSTR(v_pif_address_line_3,',')),',',''))),
	-- --ltrim(rtrim--(REPLACECHR(1,SUBSTR(v_pif_address_line_4,INSTR(v_pif_address_line_4,',')),',',''))))
	-- 
	IFF(
	    IFF(
	        LENGTH(v_pif_address_line_4)>0
	        and TRIM(v_pif_address_line_4)=''
	        or v_pif_address_line_4 IS NULL
	        or LENGTH(v_pif_address_line_4) = 0,
	        1,
	        0
	    ) = 1,
	    ltrim(rtrim(SUBSTR(v_pif_address_line_3, REGEXP_INSTR(v_pif_address_line_3, ',', - 1) + 1))),
	    ltrim(rtrim(SUBSTR(v_pif_address_line_4, REGEXP_INSTR(v_pif_address_line_4, ',', - 1) + 1)))
	) AS v_state,
	-- *INF*: REPLACECHR(1,v_state,',.',NULL)
	-- 
	-- 
	-- 
	REGEXP_REPLACE(v_state,',.','') AS v_state_replacechr,
	-- *INF*: iif(isnull(v_state_replacechr) or IS_SPACES(v_state_replacechr) or LENGTH(v_state_replacechr)=0,'N/A',ltrim(rtrim(v_state_replacechr)))
	IFF(
	    v_state_replacechr IS NULL
	    or LENGTH(v_state_replacechr)>0
	    and TRIM(v_state_replacechr)=''
	    or LENGTH(v_state_replacechr) = 0,
	    'N/A',
	    ltrim(rtrim(v_state_replacechr))
	) AS v_state_1,
	-- *INF*: IIF((v_state_1=('N/A')) AND (v_city_1=('N/A')) AND (v_address_line_2=('N/A')),LTRIM(RTRIM(SUBSTR(v_address_line_1,1,INSTR(v_address_line_1,',')-1))),v_city_1)
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(
	    (v_state_1 = ('N/A')) AND (v_city_1 = ('N/A')) AND (v_address_line_2 = ('N/A')),
	    LTRIM(RTRIM(SUBSTR(v_address_line_1, 1, REGEXP_INSTR(v_address_line_1, ',') - 1))),
	    v_city_1
	) AS v_city_1_1,
	-- *INF*: IIF((v_state_1=('N/A')) AND (v_city_1=('N/A')) AND (v_address_line_2=('N/A')),ltrim(rtrim(SUBSTR(v_address_line_1,INSTR(v_address_line_1,',',-1)+1))),v_state_1)
	-- 
	-- 
	-- --iif(iif(IS_SPACES(v_pif_address_line_4) or isnull(v_pif_address_line_4) or LENGTH(v_pif_address_line_4)=0,
	-- --1,0)=1,ltrim(rtrim(SUBSTR(v_pif_address_line_3,INSTR(v_pif_address_line_3,',',-1)+1))),ltrim(rtrim(SUBSTR(v_pif_address_line_4,INSTR(v_pif_address_line_4,',',-1)+1))))
	IFF(
	    (v_state_1 = ('N/A')) AND (v_city_1 = ('N/A')) AND (v_address_line_2 = ('N/A')),
	    ltrim(rtrim(SUBSTR(v_address_line_1, REGEXP_INSTR(v_address_line_1, ',', - 1) + 1))),
	    v_state_1
	) AS v_state_1_1,
	-- *INF*: IIF((v_city_1_1=('N/A')) AND (v_address_line_2 != ('N/A')),LTRIM(RTRIM(SUBSTR(v_address_line_2,1,INSTR(v_address_line_2,',')-1))),v_city_1_1)
	-- 
	-- 
	-- 
	IFF(
	    (v_city_1_1 = ('N/A')) AND (v_address_line_2 != ('N/A')),
	    LTRIM(RTRIM(SUBSTR(v_address_line_2, 1, REGEXP_INSTR(v_address_line_2, ',') - 1))),
	    v_city_1_1
	) AS v_final_city_name,
	-- *INF*: IIF((v_city_1_1=('N/A')) AND (v_address_line_2 != ('N/A')),ltrim(rtrim(SUBSTR(v_address_line_2,INSTR(v_address_line_2,',',-1)+1))),v_state_1_1)
	-- 
	-- 
	IFF(
	    (v_city_1_1 = ('N/A')) AND (v_address_line_2 != ('N/A')),
	    ltrim(rtrim(SUBSTR(v_address_line_2, REGEXP_INSTR(v_address_line_2, ',', - 1) + 1))),
	    v_state_1_1
	) AS v_final_state_prov_name,
	LKP_sup_state.state_code AS lkp_state_code,
	-- *INF*: iif(isnull(v_final_city_name) or IS_SPACES(v_final_city_name) or LENGTH(v_final_city_name)=0,'N/A',ltrim(rtrim(v_final_city_name)))
	-- 
	-- 
	-- 
	-- --v_city_1_1
	-- --iif(isnull(v_final_city_name) or IS_SPACES(v_final_city_name) or LENGTH(v_final_city_name)=0,'N/A',ltrim(rtrim(v_final_city_name)))
	IFF(
	    v_final_city_name IS NULL
	    or LENGTH(v_final_city_name)>0
	    and TRIM(v_final_city_name)=''
	    or LENGTH(v_final_city_name) = 0,
	    'N/A',
	    ltrim(rtrim(v_final_city_name))
	) AS city,
	-- *INF*: iif(isnull(v_final_state_prov_name) or IS_SPACES(v_final_state_prov_name) or LENGTH(v_final_state_prov_name)=0,lkp_state_code,iif(LENGTH(ltrim(rtrim(v_final_state_prov_name)))!= 2,lkp_state_code,ltrim(rtrim(v_final_state_prov_name))))
	-- 
	-- 
	-- --iif(isnull(v_final_state_prov_name) or IS_SPACES(v_final_state_prov_name) or LENGTH(v_final_state_prov_name)=0,'N/A',ltrim(rtrim(v_final_state_prov_name)))
	IFF(
	    v_final_state_prov_name IS NULL
	    or LENGTH(v_final_state_prov_name)>0
	    and TRIM(v_final_state_prov_name)=''
	    or LENGTH(v_final_state_prov_name) = 0,
	    lkp_state_code,
	    IFF(
	        LENGTH(ltrim(rtrim(v_final_state_prov_name))) != 2, lkp_state_code,
	        ltrim(rtrim(v_final_state_prov_name))
	    )
	) AS state,
	-- *INF*: iif(isnull(in_pif_zip_5_digit_code) or IS_SPACES(in_pif_zip_5_digit_code) or LENGTH(in_pif_zip_5_digit_code)=0,'N/A',LTRIM(RTRIM(in_pif_zip_5_digit_code)))
	IFF(
	    in_pif_zip_5_digit_code IS NULL
	    or LENGTH(in_pif_zip_5_digit_code)>0
	    and TRIM(in_pif_zip_5_digit_code)=''
	    or LENGTH(in_pif_zip_5_digit_code) = 0,
	    'N/A',
	    LTRIM(RTRIM(in_pif_zip_5_digit_code))
	) AS zip_postal_code,
	'N/A' AS zip_postal_code_extension,
	-- *INF*: iif(isnull(in_pif_wbc_county) or IS_SPACES(in_pif_wbc_county) or LENGTH(in_pif_wbc_county)=0,'N/A',LTRIM(RTRIM(in_pif_wbc_county)))
	IFF(
	    in_pif_wbc_county IS NULL
	    or LENGTH(in_pif_wbc_county)>0
	    and TRIM(in_pif_wbc_county)=''
	    or LENGTH(in_pif_wbc_county) = 0,
	    'N/A',
	    LTRIM(RTRIM(in_pif_wbc_county))
	) AS county_parish_name,
	'0000' AS loc_unit_num,
	-- *INF*: ltrim(rtrim(decode(v_state,
	-- 'AB','CANADA',
	-- 'BC','CANADA',
	-- 'MB','CANADA',
	-- 'MG','CANADA',
	-- 'NB','CANADA',
	-- 'NF','CANADA',
	-- 'NS','CANADA',
	-- 'NW','CANADA',
	-- 'ON','CANADA',
	-- 'PE','CANADA',
	-- 'PQ','CANADA',
	-- 'SA','CANADA',
	-- 'YN','CANADA',
	-- 'USA')))
	ltrim(rtrim(decode(
	            v_state,
	            'AB', 'CANADA',
	            'BC', 'CANADA',
	            'MB', 'CANADA',
	            'MG', 'CANADA',
	            'NB', 'CANADA',
	            'NF', 'CANADA',
	            'NS', 'CANADA',
	            'NW', 'CANADA',
	            'ON', 'CANADA',
	            'PE', 'CANADA',
	            'PQ', 'CANADA',
	            'SA', 'CANADA',
	            'YN', 'CANADA',
	            'USA'
	        ))) AS country,
	'N/A' AS no_match_flag,
	'N/A' AS delivery_confirmation_flag,
	'N/A' AS group1_match_code,
	0 AS latitude,
	0 AS longitude
	FROM SQ_pif_02_stage
	LEFT JOIN LKP_sup_state
	ON LKP_sup_state.state_abbrev = SQ_pif_02_stage.pif_risk_state_prov
),
LKP_contract_customer_key AS (
	SELECT
	contract_cust_ak_id,
	contract_key
	FROM (
		SELECT 
		contract_customer.contract_cust_ak_id as contract_cust_ak_id, 
		ltrim(rtrim(contract_customer.contract_key)) as contract_key 
		FROM 
		contract_customer
		WHERE contract_customer.crrnt_snpsht_flag=1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_key ORDER BY contract_cust_ak_id DESC) = 1
),
LKP_contract_customer_address AS (
	SELECT
	contract_cust_addr_id,
	contract_cust_addr_ak_id,
	loc_unit_num,
	addr_line_1,
	addr_line_2,
	addr_line_3,
	city_name,
	state_prov_code,
	zip_postal_code,
	zip_postal_code_extension,
	county_parish_name,
	country_name,
	no_match_flag,
	delivery_confirmation_flag,
	group1_match_code,
	latitude,
	longitude,
	contract_cust_ak_id,
	addr_type
	FROM (
		SELECT 
			contract_cust_addr_id,
			contract_cust_addr_ak_id,
			loc_unit_num,
			addr_line_1,
			addr_line_2,
			addr_line_3,
			city_name,
			state_prov_code,
			zip_postal_code,
			zip_postal_code_extension,
			county_parish_name,
			country_name,
			no_match_flag,
			delivery_confirmation_flag,
			group1_match_code,
			latitude,
			longitude,
			contract_cust_ak_id,
			addr_type
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_address
		WHERE CRRNT_SNPSHT_FLAG=1 and source_sys_id = '@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_cust_ak_id,addr_type ORDER BY contract_cust_addr_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_contract_customer_address.contract_cust_addr_id AS lkp_cust_addr_id,
	LKP_contract_customer_address.contract_cust_addr_ak_id AS lkp_cust_addr_ak_id,
	LKP_contract_customer_address.loc_unit_num AS lkp_loc_unit_num,
	LKP_contract_customer_address.addr_line_1 AS lkp_addr_line_1,
	LKP_contract_customer_address.addr_line_2 AS lkp_addr_line_2,
	LKP_contract_customer_address.addr_line_3 AS lkp_addr_line_3,
	LKP_contract_customer_address.city_name AS lkp_city,
	LKP_contract_customer_address.state_prov_code AS lkp_state,
	LKP_contract_customer_address.zip_postal_code AS lkp_zip_code,
	LKP_contract_customer_address.zip_postal_code_extension AS lkp_zip_postal_code_extension,
	LKP_contract_customer_address.county_parish_name AS lkp_county,
	LKP_contract_customer_address.country_name AS lkp_country,
	LKP_contract_customer_address.no_match_flag AS lkp_no_match_flag,
	LKP_contract_customer_address.delivery_confirmation_flag AS lkp_delivery_confirmation_flag,
	LKP_contract_customer_address.group1_match_code AS lkp_group1_match_code,
	LKP_contract_customer_address.latitude AS lkp_latitude,
	LKP_contract_customer_address.longitude AS lkp_longitude,
	EXP_values.contract_key,
	EXP_values.addr_type,
	EXP_values.customer_number,
	LKP_contract_customer_key.contract_cust_ak_id AS cust_ak_id,
	EXP_values.addr_line_1,
	EXP_values.addr_line_2,
	EXP_values.addr_line_3,
	EXP_values.city,
	EXP_values.state,
	EXP_values.zip_postal_code,
	EXP_values.zip_postal_code_extension,
	EXP_values.county_parish_name,
	EXP_values.loc_unit_num,
	EXP_values.country,
	EXP_values.no_match_flag,
	EXP_values.delivery_confirmation_flag,
	EXP_values.group1_match_code,
	EXP_values.latitude,
	EXP_values.longitude,
	-- *INF*: IIF(ISNULL(lkp_cust_addr_ak_id), 'NEW', 
	-- IIF(lkp_addr_line_1 != addr_line_1 OR
	-- lkp_addr_line_2 != addr_line_2 OR
	-- lkp_addr_line_3 != addr_line_3 OR
	-- lkp_city != city  OR
	-- lkp_state != state OR
	-- lkp_zip_code != zip_postal_code OR
	-- lkp_zip_postal_code_extension != zip_postal_code_extension OR
	-- LTRIM(RTRIM(lkp_loc_unit_num)) != loc_unit_num OR
	-- lkp_county != county_parish_name OR	
	-- lkp_country != country OR
	-- lkp_no_match_flag != no_match_flag OR
	-- lkp_delivery_confirmation_flag != delivery_confirmation_flag OR
	-- lkp_group1_match_code != group1_match_code OR
	-- lkp_latitude  != latitude OR
	-- lkp_longitude != longitude,
	-- 'UPDATE', 'NOCHANGE'))
	-- 
	-- 
	-- 
	-- --iif(NewLookupRow=1,'NEW',IIF(NewLookupRow=2,'UPDATE','NOCHANGE'))
	IFF(
	    lkp_cust_addr_ak_id IS NULL, 'NEW',
	    IFF(
	        lkp_addr_line_1 != addr_line_1
	        or lkp_addr_line_2 != addr_line_2
	        or lkp_addr_line_3 != addr_line_3
	        or lkp_city != city
	        or lkp_state != state
	        or lkp_zip_code != zip_postal_code
	        or lkp_zip_postal_code_extension != zip_postal_code_extension
	        or LTRIM(RTRIM(lkp_loc_unit_num)) != loc_unit_num
	        or lkp_county != county_parish_name
	        or lkp_country != country
	        or lkp_no_match_flag != no_match_flag
	        or lkp_delivery_confirmation_flag != delivery_confirmation_flag
	        or lkp_group1_match_code != group1_match_code
	        or lkp_latitude != latitude
	        or lkp_longitude != longitude,
	        'UPDATE',
	        'NOCHANGE'
	    )
	) AS v_changed_flag,
	v_changed_flag AS changed_flag,
	1 AS crrnt_snpsht_flag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS audit_id,
	-- *INF*: iif(v_changed_flag='NEW',
	-- 	to_date('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS'),sysdate)
	IFF(
	    v_changed_flag = 'NEW', TO_TIMESTAMP('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    CURRENT_TIMESTAMP
	) AS v_eff_from_date,
	v_eff_from_date AS eff_from_date,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS') AS eff_to_date,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_system_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date
	FROM EXP_values
	LEFT JOIN LKP_contract_customer_address
	ON LKP_contract_customer_address.contract_cust_ak_id = LKP_contract_customer_key.contract_cust_ak_id AND LKP_contract_customer_address.addr_type = EXP_values.addr_type
	LEFT JOIN LKP_contract_customer_key
	ON LKP_contract_customer_key.contract_key = EXP_values.contract_key
),
FIL_insert AS (
	SELECT
	lkp_cust_addr_ak_id, 
	contract_key, 
	addr_type, 
	customer_number, 
	cust_ak_id, 
	addr_line_1, 
	addr_line_2, 
	addr_line_3, 
	city, 
	state, 
	zip_postal_code, 
	zip_postal_code_extension, 
	county_parish_name, 
	loc_unit_num, 
	country, 
	no_match_flag, 
	delivery_confirmation_flag, 
	group1_match_code, 
	latitude, 
	longitude, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_system_id, 
	created_date, 
	modified_date
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW' OR changed_flag='UPDATE'
),
SEQ_customer_address AS (
	CREATE SEQUENCE SEQ_customer_address
	START = 0
	INCREMENT = 1;
),
EXP_customer_address_ak_id AS (
	SELECT
	lkp_cust_addr_ak_id,
	-- *INF*: IIF(ISNULL(lkp_cust_addr_ak_id),NEXTVAL,lkp_cust_addr_ak_id)
	IFF(lkp_cust_addr_ak_id IS NULL, NEXTVAL, lkp_cust_addr_ak_id) AS cust_addr_ak_id,
	contract_key,
	addr_type,
	customer_number,
	cust_ak_id,
	addr_line_1,
	addr_line_2,
	addr_line_3,
	city,
	state,
	zip_postal_code,
	zip_postal_code_extension,
	county_parish_name,
	loc_unit_num,
	country,
	no_match_flag,
	delivery_confirmation_flag,
	group1_match_code,
	latitude,
	longitude,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_system_id,
	created_date,
	modified_date,
	SEQ_customer_address.NEXTVAL
	FROM FIL_insert
),
TGT_contract_customer_address_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_address
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, contract_cust_addr_ak_id, contract_cust_ak_id, addr_type, loc_unit_num, addr_line_1, addr_line_2, addr_line_3, city_name, state_prov_code, zip_postal_code, zip_postal_code_extension, county_parish_name, country_name, no_match_flag, delivery_confirmation_flag, group1_match_code, latitude, longitude)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	source_system_id AS SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	cust_addr_ak_id AS CONTRACT_CUST_ADDR_AK_ID, 
	cust_ak_id AS CONTRACT_CUST_AK_ID, 
	ADDR_TYPE, 
	LOC_UNIT_NUM, 
	ADDR_LINE_1, 
	ADDR_LINE_2, 
	ADDR_LINE_3, 
	city AS CITY_NAME, 
	state AS STATE_PROV_CODE, 
	ZIP_POSTAL_CODE, 
	ZIP_POSTAL_CODE_EXTENSION, 
	COUNTY_PARISH_NAME, 
	country AS COUNTRY_NAME, 
	NO_MATCH_FLAG, 
	DELIVERY_CONFIRMATION_FLAG, 
	GROUP1_MATCH_CODE, 
	LATITUDE, 
	LONGITUDE
	FROM EXP_customer_address_ak_id
),
SQ_contract_customer_address AS (
	SELECT 
		contract_cust_addr_id,
		eff_from_date,
		eff_to_date,
		contract_cust_addr_ak_id 
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}. contract_customer_address 
	WHERE  contract_cust_addr_ak_id  IN
		 (SELECT contract_cust_addr_ak_id FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_address 
		   WHERE crrnt_snpsht_flag = 1 GROUP BY  contract_cust_addr_ak_id  HAVING count(*) > 1)
	AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	ORDER BY  contract_cust_addr_ak_id ,eff_from_date  DESC
),
EXP_Lag_eff_from_date AS (
	SELECT
	contract_cust_addr_id AS cust_addr_id,
	eff_from_date AS in_eff_from_date,
	eff_to_date AS orig_eff_to_date,
	contract_cust_addr_ak_id AS cust_addr_ak_id,
	-- *INF*: DECODE(TRUE,
	-- cust_addr_ak_id = v_prev_cust_addr_ak_id ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(
	    TRUE,
	    cust_addr_ak_id = v_prev_cust_addr_ak_id, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	cust_addr_ak_id AS v_prev_cust_addr_ak_id,
	in_eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_contract_customer_address
),
FIL_FirstRowInAKGroup AS (
	SELECT
	cust_addr_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_customer_address AS (
	SELECT
	cust_addr_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
TGT_contract_customer_address_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer_address AS T
	USING UPD_customer_address AS S
	ON T.contract_cust_addr_id = S.cust_addr_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),