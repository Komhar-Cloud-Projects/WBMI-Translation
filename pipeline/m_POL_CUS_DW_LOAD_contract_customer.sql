WITH
SQ_pif_02_stage AS (
	SELECT
	a.pif_symbol
	, a.pif_policy_number
	, a.pif_module
	, a.pif_line_business
	,a.pif_sort_name
	, a.pif_customer_number
	, a.wb_class_of_business
	, a.pif_address_line_1
	, a.pif_insured_name_cont
	, a.pif_address_line_2_b
	, a.pif_address_line_3
	, a.pif_address_line_4
	, a.pif_legal_entity
	,a.pif_prgm_id
	, a.pif_sic 
	, b.Pmdl4w1FedEmpIdNumber
	FROM
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_02_stage a
	LEFT JOIN
	@{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43LXZWCStage b
	ON ltrim(rtrim(a.pif_symbol)) = ltrim(rtrim(b.PifSymbol))
	and ltrim(rtrim(a.pif_policy_number))= ltrim(rtrim(b.PifPolicyNumber))
	and ltrim(rtrim(a.pif_module))=ltrim(rtrim(b.PifModule))
	and  ltrim(rtrim(b.Pmdl4w1SegmentPartCode)) = 'X'
	AND b.Pif43LXZWCStageId in (
	select max(c.Pif43LXZWCStageId) from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Pif43LXZWCStage c
	where ltrim(rtrim(b.PifSymbol))=ltrim(rtrim(c.PifSymbol))
	and ltrim(rtrim(b.PifPolicyNumber))=ltrim(rtrim(c.PifPolicyNumber))
	and ltrim(rtrim(b.PifModule))=ltrim(rtrim(c.PifModule))
	and ltrim(rtrim(c.Pmdl4w1SegmentPartCode)) = 'X')
),
LKP_pif_43jj_stage AS (
	SELECT
	pmd4j_phone_area,
	pmd4j_phone_exchange,
	pmd4j_phone_number,
	pif_symbol,
	pif_policy_number,
	pif_module
	FROM (
		SELECT 
		pmd4j_phone_area as pmd4j_phone_area, 
		pmd4j_phone_exchange as pmd4j_phone_exchange, 
		pmd4j_phone_number as pmd4j_phone_number, 
		pif_symbol as pif_symbol, 
		pif_policy_number as pif_policy_number, 
		pif_module as pif_module 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.pif_43jj_stage
		WHERE pmd4j_location_number='0001'
		ORDER BY pif_symbol,pif_policy_number,pif_module,pif_43jj_stage_id DESC--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pif_symbol,pif_policy_number,pif_module ORDER BY pmd4j_phone_area) = 1
),
EXP_values AS (
	SELECT
	SQ_pif_02_stage.pif_symbol AS in_pif_symbol,
	SQ_pif_02_stage.pif_policy_number AS in_pif_policy_number,
	SQ_pif_02_stage.pif_module AS in_pif_module,
	SQ_pif_02_stage.pif_line_business AS in_pif_line_business,
	SQ_pif_02_stage.pif_sort_name AS in_pif_sort_name,
	SQ_pif_02_stage.pif_customer_number AS in_pif_customer_number,
	SQ_pif_02_stage.wb_class_of_business AS in_wb_class_of_business,
	SQ_pif_02_stage.pif_address_line_1 AS in_pif_address_line_1,
	SQ_pif_02_stage.pif_insured_name_cont AS in_pif_insured_name_cont,
	SQ_pif_02_stage.pif_address_line_2_b AS in_pif_address_line_2_b,
	SQ_pif_02_stage.pif_address_line_3 AS in_pif_address_line_3,
	SQ_pif_02_stage.pif_address_line_4 AS in_pif_address_line_4,
	SQ_pif_02_stage.pif_legal_entity AS in_pif_legal_entity,
	SQ_pif_02_stage.pif_prgm_id AS in_pif_prgm_id,
	SQ_pif_02_stage.pif_sic AS in_pif_sic,
	SQ_pif_02_stage.Pmdl4w1FedEmpIdNumber AS in_Pmdl4w1FedEmpIdNumber,
	LKP_pif_43jj_stage.pmd4j_phone_area AS in_pmd4j_phone_area,
	LKP_pif_43jj_stage.pmd4j_phone_exchange AS in_pmd4j_phone_exchange,
	LKP_pif_43jj_stage.pmd4j_phone_number AS in_pmd4j_phone_number,
	-- *INF*: ltrim(rtrim(in_pif_symbol)) || ltrim(rtrim(in_pif_policy_number)) || ltrim(rtrim(in_pif_module))
	ltrim(rtrim(in_pif_symbol)) || ltrim(rtrim(in_pif_policy_number)) || ltrim(rtrim(in_pif_module)) AS v_contract_key,
	-- *INF*: IIF(ISNULL(in_Pmdl4w1FedEmpIdNumber) or IS_SPACES(in_Pmdl4w1FedEmpIdNumber)  or LENGTH(in_Pmdl4w1FedEmpIdNumber)=0,'N/A',LTRIM(RTRIM(in_Pmdl4w1FedEmpIdNumber)))
	IFF(
	    in_Pmdl4w1FedEmpIdNumber IS NULL
	    or LENGTH(in_Pmdl4w1FedEmpIdNumber)>0
	    and TRIM(in_Pmdl4w1FedEmpIdNumber)=''
	    or LENGTH(in_Pmdl4w1FedEmpIdNumber) = 0,
	    'N/A',
	    LTRIM(RTRIM(in_Pmdl4w1FedEmpIdNumber))
	) AS v_Pmdl4w1FedEmpIdNumber,
	-- *INF*: iif(isnull(in_wb_class_of_business) or IS_SPACES(in_wb_class_of_business)  or LENGTH(in_wb_class_of_business)=0,'N/A',ltrim(rtrim(in_wb_class_of_business)))
	-- 
	-- 
	-- 
	IFF(
	    in_wb_class_of_business IS NULL
	    or LENGTH(in_wb_class_of_business)>0
	    and TRIM(in_wb_class_of_business)=''
	    or LENGTH(in_wb_class_of_business) = 0,
	    'N/A',
	    ltrim(rtrim(in_wb_class_of_business))
	) AS v_wb_class_of_business,
	-- *INF*: iif(isnull(in_pif_customer_number) or IS_SPACES(in_pif_customer_number) or LENGTH(in_pif_customer_number)=0,'0000000000',ltrim(rtrim(in_pif_customer_number)))
	-- 
	IFF(
	    in_pif_customer_number IS NULL
	    or LENGTH(in_pif_customer_number)>0
	    and TRIM(in_pif_customer_number)=''
	    or LENGTH(in_pif_customer_number) = 0,
	    '0000000000',
	    ltrim(rtrim(in_pif_customer_number))
	) AS v_customer_num,
	'INSURED' AS v_cust_role_code,
	-- *INF*: ltrim(rtrim(in_pif_address_line_1))
	ltrim(rtrim(in_pif_address_line_1)) AS v_pif_address_line_1,
	-- *INF*: ltrim(rtrim(in_pif_address_line_2_b))
	ltrim(rtrim(in_pif_address_line_2_b)) AS v_pif_address_line_2_b,
	-- *INF*: ltrim(rtrim(in_pif_address_line_3))
	ltrim(rtrim(in_pif_address_line_3)) AS v_pif_address_line_3,
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
	-- 0)=1,v_pif_address_line_1||' '|| v_pif_insured_cont_add_line_2,v_pif_address_line_1)),
	-- iif(iif(IN(SUBSTR(v_pif_address_line_2_b,1,1),'1','2','3','4','5','6','7','8','9','0',0)=1,1,iif(decode(substr(v_pif_insured_cont_add_line_2,1,4),
	-- 'POST',1,
	-- 'PO B',1,0)=1,1,0))=0,
	-- iif(decode(SUBSTR(v_pif_address_line_2_b,(LENGTH(v_pif_address_line_2_b)-2),3) ,
	-- ' ST',1,
	-- ' RD',1,
	-- ' PL',1,0)=1,v_pif_address_line_1,v_pif_address_line_1||' '|| v_pif_insured_cont_add_line_2),
	-- iif(iif(decode(SUBSTR(v_pif_address_line_2_b,(LENGTH(v_pif_address_line_2_b)-3),4),
	-- ' LLC',1,
	-- ' DBA',1,
	-- ' BA;',1,
	-- ' BA,',1,
	-- ' SOC',1,
	-- 'PLAN',1,
	-- 'CORP',1,
	-- 0)=1,1,iif(IIF(NOT (ISNULL(v_pif_address_line_2_b)) OR NOT (IS_SPACES(v_pif_address_line_2_b)), instr(v_pif_address_line_2_b,'TRUST'),0) != 0,1,iif(IIF(NOT (ISNULL(v_pif_address_line_1)) OR NOT (IS_SPACES(v_pif_address_line_1)), instr(v_pif_address_line_1,'TRUST'),0) != 0,1,0))) =1,v_pif_address_line_1||' '|| v_pif_insured_cont_add_line_2,v_pif_address_line_1)))
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
	            v_pif_address_line_1 || ' ' || v_pif_insured_cont_add_line_2,
	            v_pif_address_line_1
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
	            v_pif_address_line_1,
	            v_pif_address_line_1 || ' ' || v_pif_insured_cont_add_line_2
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
	            v_pif_address_line_1 || ' ' || v_pif_insured_cont_add_line_2,
	            v_pif_address_line_1
	        )
	    )
	) AS v_name,
	-- *INF*: iif(isnull(in_pif_prgm_id) or is_spaces(in_pif_prgm_id) or LENGTH(in_pif_prgm_id)=0,'N/A',LTRIM(RTRIM(in_pif_prgm_id)))
	IFF(
	    in_pif_prgm_id IS NULL
	    or LENGTH(in_pif_prgm_id)>0
	    and TRIM(in_pif_prgm_id)=''
	    or LENGTH(in_pif_prgm_id) = 0,
	    'N/A',
	    LTRIM(RTRIM(in_pif_prgm_id))
	) AS v_pif_prgm_id,
	-- *INF*: --:LKP.LKP_SUP_LEGAL_ENTITY(in_pif_legal_entity)
	'' AS v_sup_legal_entity_id,
	-- *INF*: --:LKP.LKP_SUP_SIC_CODE(ltrim(rtrim(in_pif_sic)))
	'' AS v_sup_sic_code_id,
	-- *INF*: IIF(LTRIM(RTRIM(in_pif_line_business)) = 'WC' OR LTRIM(RTRIM(in_pif_line_business)) = 'WCP', v_Pmdl4w1FedEmpIdNumber, 'N/A')
	IFF(
	    LTRIM(RTRIM(in_pif_line_business)) = 'WC' OR LTRIM(RTRIM(in_pif_line_business)) = 'WCP',
	    v_Pmdl4w1FedEmpIdNumber,
	    'N/A'
	) AS v_fed_tax_id,
	-- *INF*: LTRIM(RTRIM(in_pmd4j_phone_area))||LTRIM(RTRIM(in_pmd4j_phone_exchange))||LTRIM(RTRIM(in_pmd4j_phone_number))
	LTRIM(RTRIM(in_pmd4j_phone_area)) || LTRIM(RTRIM(in_pmd4j_phone_exchange)) || LTRIM(RTRIM(in_pmd4j_phone_number)) AS v_ph_num_full,
	-- *INF*: ltrim(rtrim(v_contract_key))
	ltrim(rtrim(v_contract_key)) AS o_contract_key,
	-- *INF*: ---iif(ltrim(rtrim(v_sup_assoc_program_type))='Other',v_wb_class_of_business,'N/A')
	'' AS o_Pol_other_clb_code,
	-- *INF*: LTRIM(RTRIM(v_cust_role_code))
	LTRIM(RTRIM(v_cust_role_code)) AS o_cust_role,
	v_customer_num AS o_customer_number,
	-- *INF*: iif(isnull(v_name) or IS_SPACES(v_name) or LENGTH(v_name)=0,'N/A',LTRIM(RTRIM(v_name)))
	IFF(
	    v_name IS NULL or LENGTH(v_name)>0 AND TRIM(v_name)='' or LENGTH(v_name) = 0, 'N/A',
	    LTRIM(RTRIM(v_name))
	) AS o_name,
	-- *INF*: iif(isnull(in_pif_legal_entity) or IS_SPACES(in_pif_legal_entity)  or LENGTH(in_pif_legal_entity)=0,'N/A',ltrim(rtrim(in_pif_legal_entity)))
	IFF(
	    in_pif_legal_entity IS NULL
	    or LENGTH(in_pif_legal_entity)>0
	    and TRIM(in_pif_legal_entity)=''
	    or LENGTH(in_pif_legal_entity) = 0,
	    'N/A',
	    ltrim(rtrim(in_pif_legal_entity))
	) AS o_lgl_ent_code,
	-- *INF*: iif(isnull(in_pif_sic) or IS_SPACES(in_pif_sic)  or LENGTH(in_pif_sic)=0,'N/A',ltrim(rtrim(in_pif_sic)))
	-- 
	IFF(
	    in_pif_sic IS NULL or LENGTH(in_pif_sic)>0 AND TRIM(in_pif_sic)='' or LENGTH(in_pif_sic) = 0,
	    'N/A',
	    ltrim(rtrim(in_pif_sic))
	) AS o_sic_code,
	-- *INF*: ltrim(rtrim(in_pif_sort_name))
	ltrim(rtrim(in_pif_sort_name)) AS o_pif_sort_name,
	'N/A' AS o_naics_code,
	v_fed_tax_id AS o_fed_tax_id,
	'N/A' AS o_doing_bus_as,
	-1 AS o_yr_in_bus,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(v_ph_num_full)
	UDF_DEFAULT_VALUE_FOR_STRINGS(v_ph_num_full) AS o_ph_num_full,
	'N/A' AS o_ph_area_code,
	'N/A' AS o_ph_exchange,
	'N/A' AS o_ph_num,
	'N/A' AS o_ph_extension,
	'N/A' AS o_bus_email_addr
	FROM SQ_pif_02_stage
	LEFT JOIN LKP_pif_43jj_stage
	ON LKP_pif_43jj_stage.pif_symbol = SQ_pif_02_stage.pif_symbol AND LKP_pif_43jj_stage.pif_policy_number = SQ_pif_02_stage.pif_policy_number AND LKP_pif_43jj_stage.pif_module = SQ_pif_02_stage.pif_module
),
LKP_contract_customer AS (
	SELECT
	contract_cust_id,
	contract_cust_ak_id,
	cust_num,
	name,
	fed_tax_id,
	doing_bus_as,
	sic_code,
	naics_code,
	lgl_ent_code,
	yr_in_bus,
	ph_num_full,
	ph_area_code,
	ph_exchange,
	ph_num,
	ph_extension,
	bus_email_addr,
	sort_name,
	sup_lgl_ent_code_id,
	contract_key,
	cust_role
	FROM (
		SELECT 
			contract_cust_id,
			contract_cust_ak_id,
			cust_num,
			name,
			fed_tax_id,
			doing_bus_as,
			sic_code,
			naics_code,
			lgl_ent_code,
			yr_in_bus,
			ph_num_full,
			ph_area_code,
			ph_exchange,
			ph_num,
			ph_extension,
			bus_email_addr,
			sort_name,
			sup_lgl_ent_code_id,
			contract_key,
			cust_role
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer
		WHERE contract_customer.crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY contract_key,cust_role ORDER BY contract_cust_id) = 1
),
LKP_sup_legal_entity_code AS (
	SELECT
	sup_lgl_ent_code_id,
	lgl_ent_code
	FROM (
		SELECT 
			sup_lgl_ent_code_id,
			lgl_ent_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_legal_entity_code
		WHERE crrnt_snpsht_flag=1 and source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY lgl_ent_code ORDER BY sup_lgl_ent_code_id) = 1
),
EXP_Detect_Changes AS (
	SELECT
	LKP_contract_customer.contract_cust_id AS lkp_cust_id,
	LKP_contract_customer.contract_cust_ak_id AS lkp_cust_ak_id,
	LKP_contract_customer.cust_num AS lkp_cust_num,
	LKP_contract_customer.name AS lkp_name,
	LKP_contract_customer.fed_tax_id AS lkp_fed_tax_id,
	LKP_contract_customer.doing_bus_as AS lkp_doing_bus_as,
	LKP_contract_customer.sic_code AS lkp_sic_code,
	LKP_contract_customer.naics_code AS lkp_naics_code,
	LKP_contract_customer.lgl_ent_code AS lkp_lgl_ent_code,
	LKP_contract_customer.yr_in_bus AS lkp_yr_in_bus,
	LKP_contract_customer.ph_num_full AS lkp_ph_num_full,
	LKP_contract_customer.ph_area_code AS lkp_ph_area_code,
	LKP_contract_customer.ph_exchange AS lkp_ph_exchange,
	LKP_contract_customer.ph_num AS lkp_ph_num,
	LKP_contract_customer.ph_extension AS lkp_ph_extension,
	LKP_contract_customer.bus_email_addr AS lkp_bus_email_addr,
	LKP_contract_customer.sort_name AS lkp_sort_name,
	LKP_contract_customer.sup_lgl_ent_code_id AS lkp_sup_lgl_ent_code_id,
	EXP_values.o_contract_key AS contract_key,
	EXP_values.o_cust_role AS cust_role,
	EXP_values.o_customer_number AS customer_number,
	EXP_values.o_name AS name,
	EXP_values.o_fed_tax_id AS fed_tax_id,
	EXP_values.o_doing_bus_as AS doing_bus_as,
	EXP_values.o_sic_code AS sic_code,
	EXP_values.o_naics_code AS naics_code,
	EXP_values.o_lgl_ent_code AS lgl_ent_code,
	EXP_values.o_yr_in_bus AS yr_in_bus,
	EXP_values.o_ph_num_full AS ph_num_full,
	EXP_values.o_ph_area_code AS ph_area_code,
	EXP_values.o_ph_exchange AS ph_exchange,
	EXP_values.o_ph_num AS ph_num,
	EXP_values.o_ph_extension AS ph_extension,
	EXP_values.o_bus_email_addr AS bus_email_addr,
	EXP_values.o_pif_sort_name AS pif_sort_name,
	LKP_sup_legal_entity_code.sup_lgl_ent_code_id AS in_sup_lgl_ent_code_id,
	-- *INF*: IIF(
	--   ISNULL(in_sup_lgl_ent_code_id),
	--   -1,
	--   in_sup_lgl_ent_code_id
	-- )
	IFF(in_sup_lgl_ent_code_id IS NULL, - 1, in_sup_lgl_ent_code_id) AS v_sup_lgl_ent_code_id,
	-- *INF*: IIF(ISNULL(lkp_cust_ak_id), 'NEW', IIF(
	-- lkp_cust_num != customer_number OR
	-- lkp_name != name OR
	-- lkp_fed_tax_id != fed_tax_id OR
	-- lkp_doing_bus_as != doing_bus_as  OR
	-- lkp_sic_code != sic_code  OR
	-- lkp_naics_code != naics_code  OR
	-- lkp_lgl_ent_code != lgl_ent_code  OR 
	-- lkp_yr_in_bus != yr_in_bus OR
	-- lkp_ph_num_full != ph_num_full OR
	-- lkp_ph_area_code != ph_area_code OR	
	-- lkp_ph_exchange != ph_area_code OR
	-- lkp_ph_num != ph_num OR
	-- lkp_ph_extension != ph_extension OR
	-- lkp_bus_email_addr != bus_email_addr OR
	-- LTRIM(RTRIM(lkp_sort_name)) != pif_sort_name
	-- 
	-- --OR
	-- --lkp_ProgramCode != v_ProgramCode
	-- --OR lkp_StandardLegalEntityCode != v_StandardLegalEntityCode OR lkp_StandardLegalEntityDescription != v_StandardLegalEntityDescription OR lkp_StandardAssociationCode != v_StandardAssociationCode OR lkp_StandardAssociationDescription != v_StandardAssociationDescription OR lkp_StandardProgramCode != v_StandardProgramCode OR lkp_StandardProgramDescription != v_StandardProgramDescription
	-- ,
	-- 'UPDATE', 'NOCHANGE'))
	-- 
	-- 
	-- --IIF(NewLookupRow=1,'NEW',IIF(NewLookupRow=2,'UPDATE','NOCHANGE'
	-- 
	IFF(
	    lkp_cust_ak_id IS NULL, 'NEW',
	    IFF(
	        lkp_cust_num != customer_number
	        or lkp_name != name
	        or lkp_fed_tax_id != fed_tax_id
	        or lkp_doing_bus_as != doing_bus_as
	        or lkp_sic_code != sic_code
	        or lkp_naics_code != naics_code
	        or lkp_lgl_ent_code != lgl_ent_code
	        or lkp_yr_in_bus != yr_in_bus
	        or lkp_ph_num_full != ph_num_full
	        or lkp_ph_area_code != ph_area_code
	        or lkp_ph_exchange != ph_area_code
	        or lkp_ph_num != ph_num
	        or lkp_ph_extension != ph_extension
	        or lkp_bus_email_addr != bus_email_addr
	        or LTRIM(RTRIM(lkp_sort_name)) != pif_sort_name,
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
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS source_sys_id,
	SYSDATE AS created_date,
	SYSDATE AS modified_date,
	v_sup_lgl_ent_code_id AS o_sup_lgl_ent_code_id
	FROM EXP_values
	LEFT JOIN LKP_contract_customer
	ON LKP_contract_customer.contract_key = EXP_values.o_contract_key AND LKP_contract_customer.cust_role = EXP_values.o_cust_role
	LEFT JOIN LKP_sup_legal_entity_code
	ON LKP_sup_legal_entity_code.lgl_ent_code = EXP_values.o_lgl_ent_code
),
FIL_insert AS (
	SELECT
	lkp_cust_ak_id, 
	contract_key, 
	cust_role, 
	customer_number, 
	name, 
	fed_tax_id, 
	doing_bus_as, 
	sic_code, 
	naics_code, 
	lgl_ent_code, 
	yr_in_bus, 
	ph_num_full, 
	ph_area_code, 
	ph_exchange, 
	ph_num, 
	ph_extension, 
	bus_email_addr, 
	pif_sort_name, 
	changed_flag, 
	crrnt_snpsht_flag, 
	audit_id, 
	eff_from_date, 
	eff_to_date, 
	source_sys_id, 
	created_date, 
	modified_date, 
	o_sup_lgl_ent_code_id AS sup_lgl_ent_code_id
	FROM EXP_Detect_Changes
	WHERE changed_flag='NEW'  OR changed_flag='UPDATE'
),
SEQ_customer AS (
	CREATE SEQUENCE SEQ_customer
	START = 0
	INCREMENT = 1;
),
EXP_customer_ak_id AS (
	SELECT
	lkp_cust_ak_id,
	-- *INF*: IIF(ISNULL(lkp_cust_ak_id),NEXTVAL,lkp_cust_ak_id)
	IFF(lkp_cust_ak_id IS NULL, NEXTVAL, lkp_cust_ak_id) AS cust_ak_id,
	contract_key,
	cust_role,
	customer_number,
	name,
	fed_tax_id,
	doing_bus_as,
	sic_code,
	naics_code,
	lgl_ent_code,
	yr_in_bus,
	ph_num_full,
	ph_area_code,
	ph_exchange,
	ph_num,
	ph_extension,
	bus_email_addr,
	pif_sort_name,
	crrnt_snpsht_flag,
	audit_id,
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	modified_date,
	SEQ_customer.NEXTVAL,
	sup_lgl_ent_code_id,
	-- *INF*: 'N/A'
	-- -- Default values for PMS
	'N/A' AS DEFAULT
	FROM FIL_insert
),
TGT_contract_customer_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer
	(crrnt_snpsht_flag, audit_id, eff_from_date, eff_to_date, source_sys_id, created_date, modified_date, contract_cust_ak_id, cust_num, contract_key, cust_role, name, fed_tax_id, doing_bus_as, sic_code, naics_code, lgl_ent_code, yr_in_bus, ph_num_full, ph_area_code, ph_exchange, ph_num, ph_extension, bus_email_addr, sort_name, sup_lgl_ent_code_id, FirstName, LastName, MiddleName)
	SELECT 
	CRRNT_SNPSHT_FLAG, 
	AUDIT_ID, 
	EFF_FROM_DATE, 
	EFF_TO_DATE, 
	SOURCE_SYS_ID, 
	CREATED_DATE, 
	MODIFIED_DATE, 
	cust_ak_id AS CONTRACT_CUST_AK_ID, 
	customer_number AS CUST_NUM, 
	CONTRACT_KEY, 
	CUST_ROLE, 
	NAME, 
	FED_TAX_ID, 
	DOING_BUS_AS, 
	SIC_CODE, 
	NAICS_CODE, 
	LGL_ENT_CODE, 
	YR_IN_BUS, 
	PH_NUM_FULL, 
	PH_AREA_CODE, 
	PH_EXCHANGE, 
	PH_NUM, 
	PH_EXTENSION, 
	BUS_EMAIL_ADDR, 
	pif_sort_name AS SORT_NAME, 
	SUP_LGL_ENT_CODE_ID, 
	DEFAULT AS FIRSTNAME, 
	DEFAULT AS LASTNAME, 
	DEFAULT AS MIDDLENAME
	FROM EXP_customer_ak_id
),
SQ_contract_customer AS (
	SELECT 
		contract_cust_id,
		eff_from_date,
		eff_to_date,
		contract_cust_ak_id 
	FROM
		@{pipeline().parameters.TARGET_TABLE_OWNER}. contract_customer
	WHERE  contract_cust_ak_id  IN 
		   (SELECT contract_cust_ak_id  FROM @{pipeline().parameters.TARGET_TABLE_OWNER}. contract_customer
	           WHERE crrnt_snpsht_flag = 1 GROUP BY contract_cust_ak_id  HAVING count(*) > 1)
	AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	ORDER BY  contract_cust_ak_id , eff_from_date  DESC
	
	--IN Subquery exists to pick AK ID column values that have multiple rows with a 12/31/2100 eff_to_date.
	--When this condition occurs this is an indication that we must expire one or more of these rows.
	--WHERE clause is always made up of current snapshot flag 
	--GROUP BY clause is always on AK
	--HAVING clause stays the same
),
EXP_Lag_eff_from_date AS (
	SELECT
	contract_cust_id AS cust_id,
	eff_from_date AS in_eff_from_date,
	eff_to_date AS orig_eff_to_date,
	contract_cust_ak_id AS cust_ak_id,
	-- *INF*: DECODE(TRUE,
	-- cust_ak_id = v_prev_cust_ak_id  ,
	-- ADD_TO_DATE(v_prev_eff_from_date,'SS',-1),orig_eff_to_date)
	DECODE(
	    TRUE,
	    cust_ak_id = v_prev_cust_ak_id, DATEADD(SECOND,- 1,v_prev_eff_from_date),
	    orig_eff_to_date
	) AS v_eff_to_date,
	v_eff_to_date AS eff_to_date,
	cust_ak_id AS v_prev_cust_ak_id,
	in_eff_from_date AS v_prev_eff_from_date,
	0 AS crrnt_snpsht_flag,
	SYSDATE AS modified_date
	FROM SQ_contract_customer
),
FIL_FirstRowInAKGroup AS (
	SELECT
	cust_id, 
	orig_eff_to_date, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM EXP_Lag_eff_from_date
	WHERE orig_eff_to_date != eff_to_date
),
UPD_customer AS (
	SELECT
	cust_id, 
	eff_to_date, 
	crrnt_snpsht_flag, 
	modified_date
	FROM FIL_FirstRowInAKGroup
),
TGT_contract_customer_UPDATE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.contract_customer AS T
	USING UPD_customer AS S
	ON T.contract_cust_id = S.cust_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.crrnt_snpsht_flag = S.crrnt_snpsht_flag, T.eff_to_date = S.eff_to_date, T.modified_date = S.modified_date
),