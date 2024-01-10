WITH
SQ_WorkNcciMitchell AS (
	SELECT Distinct  WorkNcciMitchell.SocialSecurityNumber from WorkNcciMitchell
	@{pipeline().parameters.WHERE_WORKNCCIMITCHELL}
),
EXP_WorkNcciMitchell AS (
	SELECT
	SocialSecurityNumber
	FROM SQ_WorkNcciMitchell
),
SQ_arch_pif_42gj_stage AS (
	SELECT Distinct arch_pif_42gj_stage.ipfc4j_id_number 
	FROM
	 arch_pif_42gj_stage
	@{pipeline().parameters.WHERE_ARCH_PIF_42GJ_STAGE}
),
EXP_arch_pif_42gj_stage AS (
	SELECT
	ipfc4j_id_number
	FROM SQ_arch_pif_42gj_stage
),
SQ_arch_pms_adjuster_master_stage AS (
	SELECT Distinct arch_pms_adjuster_master_stage.adnm_taxid_ssn 
	FROM
	 arch_pms_adjuster_master_stage
	@{pipeline().parameters.WHERE_ARCH_PMS_ADJUSTER_MASTER_STAGE}
),
EXP_arch_pms_adjuster_master_stage AS (
	SELECT
	adnm_taxid_ssn
	FROM SQ_arch_pms_adjuster_master_stage
),
SQ_claim_loss_transaction_fact AS (
	select Distinct  claim_loss_transaction_fact.tax_id from claim_loss_transaction_fact
	@{pipeline().parameters.WHERE_CLAIM_LOSS_TRANSACTION_FACT}
),
EXP_claim_loss_transaction_fact AS (
	SELECT
	tax_id
	FROM SQ_claim_loss_transaction_fact
),
SQ_claim_party AS (
	SELECT Distinct  claim_party.tax_ssn_id FROM claim_party
	@{pipeline().parameters.WHERE_CLAIM_PARTY}
),
EXP_claim_party AS (
	SELECT
	tax_ssn_id
	FROM SQ_claim_party
),
SQ_claim_party_dim AS (
	select  Distinct claim_party_dim.tax_ssn_id from claim_party_dim
	@{pipeline().parameters.WHERE_CLAIM_PARTY_DIM}
),
EXP_claim_party_dim AS (
	SELECT
	tax_ssn_id
	FROM SQ_claim_party_dim
),
SQ_claim_payment_dim AS (
	select  Distinct  claim_payment_dim.prim_payee_tax_id from claim_payment_dim
	@{pipeline().parameters.WHERE_CLAIM_PAYMENT_DIM}
),
EXP_claim_payment_dim AS (
	SELECT
	prim_payee_tax_id
	FROM SQ_claim_payment_dim
),
SQ_claim_transaction AS (
	select Distinct claim_transaction.tax_id from claim_transaction
	@{pipeline().parameters.WHERE_CLAIM_TRANSACTION}
),
EXP_claim_transaction AS (
	SELECT
	tax_id
	FROM SQ_claim_transaction
),
SQ_claimant_dim AS (
	select Distinct  claimant_dim.claimant_tax_ssn_id from claimant_dim
	@{pipeline().parameters.WHERE_CLAIMANT_DIM}
),
EXP_claimant_dim AS (
	SELECT
	claimant_tax_ssn_id
	FROM SQ_claimant_dim
),
SQ_pif_42gj_stage AS (
	select Distinct  pif_42gj_stage.ipfc4j_id_number from pif_42gj_stage
	@{pipeline().parameters.WHERE_PIF_42GJ_STAGE}
),
EXP_pif_42gj_stage AS (
	SELECT
	ipfc4j_id_number
	FROM SQ_pif_42gj_stage
),
SQ_pms_adjuster_master_stage AS (
	select Distinct pms_adjuster_master_stage.adnm_taxid_ssn from pms_adjuster_master_stage
	@{pipeline().parameters.WHERE_PMS_ADJUSTER_MASTER_STAGE}
),
EXP_pms_adjuster_master_stage AS (
	SELECT
	adnm_taxid_ssn
	FROM SQ_pms_adjuster_master_stage
),
SQ_work_claim_cms_detail_extract AS (
	SELECT  Distinct work_claim_cms_detail_extract.injured_party_ssn FROM work_claim_cms_detail_extract
	@{pipeline().parameters.WHERE_WORK_CLAIM_CMS_DETAIL_EXTRACT}
),
EXP_work_claim_cms_detail_extract AS (
	SELECT
	injured_party_ssn
	FROM SQ_work_claim_cms_detail_extract
),
SQ_work_claim_cms_query_extract AS (
	SELECT Distinct  work_claim_cms_query_extract.injured_party_ssn 
	FROM
	 work_claim_cms_query_extract
	@{pipeline().parameters.WHERE_WORK_CLAIM_CMS_QUERY_EXTRACT}
),
EXP_work_claim_cms_query_extract AS (
	SELECT
	injured_party_ssn
	FROM SQ_work_claim_cms_query_extract
),
SQ_work_claim_matters_extract AS (
	SELECT  Distinct work_claim_matters_extract.matter_firm_tax_id FROM work_claim_matters_extract
	@{pipeline().parameters.WHERE_WORK_CLAIM_MATTERS_EXTRACT}
),
EXP_work_claim_matters_extract AS (
	SELECT
	matter_firm_tax_id
	FROM SQ_work_claim_matters_extract
),
SQ_work_workers_comp_first_report_of_injury_extract AS (
	SELECT  Distinct work_workers_comp_first_report_of_injury_extract.employee_ssn 
	FROM
	 work_workers_comp_first_report_of_injury_extract
	@{pipeline().parameters.WHERE_WORK_WORKERS_COMP_FIRST_REPORT_OF_INJURY_EXTRACT}
),
EXP_work_workers_comp_first_report_of_injury_extract AS (
	SELECT
	employee_ssn
	FROM SQ_work_workers_comp_first_report_of_injury_extract
),
Union_Input_Values AS (
	SELECT adnm_taxid_ssn
	FROM EXP_pms_adjuster_master_stage
	UNION
	SELECT ipfc4j_id_number AS adnm_taxid_ssn
	FROM EXP_pif_42gj_stage
	UNION
	SELECT matter_firm_tax_id AS adnm_taxid_ssn
	FROM EXP_work_claim_matters_extract
	UNION
	SELECT injured_party_ssn AS adnm_taxid_ssn
	FROM EXP_work_claim_cms_detail_extract
	UNION
	SELECT tax_ssn_id AS adnm_taxid_ssn
	FROM EXP_claim_party
	UNION
	SELECT SocialSecurityNumber AS adnm_taxid_ssn
	FROM EXP_WorkNcciMitchell
	UNION
	SELECT prim_payee_tax_id AS adnm_taxid_ssn
	FROM EXP_claim_payment_dim
	UNION
	SELECT tax_ssn_id AS adnm_taxid_ssn
	FROM EXP_claim_party_dim
	UNION
	SELECT claimant_tax_ssn_id AS adnm_taxid_ssn
	FROM EXP_claimant_dim
	UNION
	SELECT tax_id AS adnm_taxid_ssn
	FROM EXP_claim_loss_transaction_fact
	UNION
	SELECT ipfc4j_id_number AS adnm_taxid_ssn
	FROM EXP_arch_pif_42gj_stage
	UNION
	SELECT tax_id AS adnm_taxid_ssn
	FROM EXP_claim_transaction
	UNION
	SELECT employee_ssn AS adnm_taxid_ssn
	FROM EXP_work_workers_comp_first_report_of_injury_extract
	UNION
	SELECT adnm_taxid_ssn
	FROM EXP_arch_pms_adjuster_master_stage
	UNION
	SELECT injured_party_ssn AS adnm_taxid_ssn
	FROM EXP_work_claim_cms_query_extract
),
mplt_SSN_Check AS (WITH
	INPUT AS (
		
	),
	EXP_SSN_FEIN_TAXID AS (
		SELECT
		IN_id AS id,
		IN_ssn_fein_id AS ssn_fein_taxid,
		-- *INF*: LTRIM(RTRIM(ssn_fein_taxid))
		LTRIM(RTRIM(ssn_fein_taxid
			)
		) AS V_ssn_fein_taxid,
		-- *INF*: IIF( SUBSTR(V_ssn_fein_taxid,3,1)='-' OR  (SUBSTR(V_ssn_fein_taxid,1,3)='000' AND (LENGTH(V_ssn_fein_taxid)=9 ) ) OR SUBSTR(V_ssn_fein_taxid,2,1)='-'  OR   (TO_INTEGER(SUBSTR(V_ssn_fein_taxid,1,3))>=750 AND (LENGTH(V_ssn_fein_taxid)=9 ) )OR ISNULL(V_ssn_fein_taxid) OR (V_ssn_fein_taxid='N/A')   OR REG_MATCH(V_ssn_fein_taxid,'[*]*') OR(REG_MATCH(V_ssn_fein_taxid,'[\da-zA-Z]+') AND (LENGTH(V_ssn_fein_taxid)=11 OR LENGTH(V_ssn_fein_taxid)=10) )
		--  OR ((SUBSTR(V_ssn_fein_taxid,4,1)='-')  AND  (LENGTH(V_ssn_fein_taxid) != 11 )) OR (LENGTH(V_ssn_fein_taxid)<=6 ) OR  (LENGTH(V_ssn_fein_taxid)>11 ) ,'FEIN','NONFEIN')
		-- 
		-- 
		-- 
		-- 
		-- 
		IFF(SUBSTR(V_ssn_fein_taxid, 3, 1
			) = '-' 
			OR ( SUBSTR(V_ssn_fein_taxid, 1, 3
				) = '000' 
				AND ( LENGTH(V_ssn_fein_taxid
					) = 9 
				) 
			) 
			OR SUBSTR(V_ssn_fein_taxid, 2, 1
			) = '-' 
			OR ( CAST(SUBSTR(V_ssn_fein_taxid, 1, 3
				) AS INTEGER) >= 750 
				AND ( LENGTH(V_ssn_fein_taxid
					) = 9 
				) 
			) 
			OR V_ssn_fein_taxid IS NULL 
			OR ( V_ssn_fein_taxid = 'N/A' 
			) 
			OR REGEXP_LIKE(V_ssn_fein_taxid, '[*]*'
			) 
			OR ( REGEXP_LIKE(V_ssn_fein_taxid, '[\da-zA-Z]+'
				) 
				AND ( LENGTH(V_ssn_fein_taxid
					) = 11 
					OR LENGTH(V_ssn_fein_taxid
					) = 10 
				) 
			) 
			OR ( ( SUBSTR(V_ssn_fein_taxid, 4, 1
					) = '-' 
				) 
				AND ( LENGTH(V_ssn_fein_taxid
					) != 11 
				) 
			) 
			OR ( LENGTH(V_ssn_fein_taxid
				) <= 6 
			) 
			OR ( LENGTH(V_ssn_fein_taxid
				) > 11 
			),
			'FEIN',
			'NONFEIN'
		) AS V_flag,
		V_flag AS flag,
		-- *INF*: IIF(LENGTH(V_ssn_fein_taxid)>=7 AND LENGTH(V_ssn_fein_taxid)<=8,LPAD(V_ssn_fein_taxid,9,'0'),V_ssn_fein_taxid)
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		IFF(LENGTH(V_ssn_fein_taxid
			) >= 7 
			AND LENGTH(V_ssn_fein_taxid
			) <= 8,
			LPAD(V_ssn_fein_taxid, 9, '0'
			),
			V_ssn_fein_taxid
		) AS V_taxid,
		-- *INF*: IIF(REG_MATCH(V_taxid,'[0-9-]*') ,V_taxid,'X')
		-- 
		-- 
		-- 
		IFF(REGEXP_LIKE(V_taxid, '[0-9-]*'
			),
			V_taxid,
			'X'
		) AS V_valid_taxid,
		V_valid_taxid AS flag_TaxId,
		-- *INF*: IIF(LENGTH(V_valid_taxid)=9  AND (REG_MATCH(V_valid_taxid,'^[0-9]*$'))  ,(SUBSTR(V_valid_taxid, 1, 3) ||'-'||SUBSTR(V_valid_taxid, 4, 2)||'-'||SUBSTR(V_valid_taxid, 6, 4)) ,V_valid_taxid)
		-- 
		-- 
		-- 
		-- 
		IFF(LENGTH(V_valid_taxid
			) = 9 
			AND ( REGEXP_LIKE(V_valid_taxid, '^[0-9]*$'
				) 
			),
			( SUBSTR(V_valid_taxid, 1, 3
				) || '-' || SUBSTR(V_valid_taxid, 4, 2
				) || '-' || SUBSTR(V_valid_taxid, 6, 4
				) 
			),
			V_valid_taxid
		) AS OUT_taxid
		FROM INPUT
	),
	RTR_SSN_FEIN_TAXID AS (
		SELECT
		id,
		flag,
		ssn_fein_taxid AS fein_taxid,
		OUT_taxid AS ssn,
		flag_TaxId
		FROM EXP_SSN_FEIN_TAXID
	),
	RTR_SSN_FEIN_TAXID_FEIN AS (SELECT * FROM RTR_SSN_FEIN_TAXID WHERE flag = 'FEIN'),
	RTR_SSN_FEIN_TAXID_SSN AS (SELECT * FROM RTR_SSN_FEIN_TAXID WHERE flag='NONFEIN'  AND flag_TaxId != 'X'),
	AGGTRANS AS (
		SELECT
		id AS Id,
		ssn AS SSN
		FROM RTR_SSN_FEIN_TAXID_SSN
		QUALIFY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY NULL) = 1
	),
	OUTPUT AS (
		SELECT
		Id AS OUT_id, 
		SSN AS OUT_valid_ssn
		FROM AGGTRANS
	),
),
AGG_Remove_Duplicates AS (
	SELECT
	OUT_id AS o_ID,
	OUT_valid_ssn AS adnm_taxid_ssn
	FROM mplt_SSN_Check
	QUALIFY ROW_NUMBER() OVER (PARTITION BY o_ID, adnm_taxid_ssn ORDER BY NULL) = 1
),
SQ_arch_claim_draft_stage AS (
	select Distinct dft_tax_id_nbr from arch_claim_draft_stage
	where dft_tax_id_type_cd='S' AND  @{pipeline().parameters.WHERE_ARCH_CLAIM_DRAFT_STAGE}
),
EXP_ARCH_CLAIM_DRAFT_STAGE AS (
	SELECT
	dft_tax_id_nbr
	FROM SQ_arch_claim_draft_stage
),
SQ_CLAIM_DRAFT_STAGE AS (
	SELECT Distinct dft_tax_id_nbr FROM claim_draft_stage
	WHERE dft_tax_id_type_cd='S' AND  @{pipeline().parameters.WHERE_CLAIM_DRAFT_STAGE}
),
EXP_CLAIM_DRAFT_STAGE AS (
	SELECT
	DFT_TAX_ID_NBR AS dft_tax_id_nbr
	FROM SQ_CLAIM_DRAFT_STAGE
),
SQ_ClaimDraftMonthlyStage AS (
	SELECT Distinct ClaimDraftMonthlyStage.dft_tax_id_nbr 
	FROM
	 ClaimDraftMonthlyStage
	where ClaimDraftMonthlyStage.dft_tax_id_type_cd='S' AND @{pipeline().parameters.WHERE_CLAIMDRAFTMONTHLYSTAGE}
),
EXP_ClaimDraftMonthlyStage AS (
	SELECT
	dft_tax_id_nbr
	FROM SQ_ClaimDraftMonthlyStage
),
SQ_Master1099ListMonthlyStage AS (
	select Distinct  Master1099ListMonthlyStage.tax_id
	from Master1099ListMonthlyStage
	where Master1099ListMonthlyStage.tax_id_type = 'S' AND @{pipeline().parameters.WHERE_MASTER1099LISTMONTHLYSTAGE}
	UNION
	select Distinct Master1099ListMonthlyStage.search_tax_id 
	from Master1099ListMonthlyStage
	where Master1099ListMonthlyStage.tax_id_type = 'S' AND @{pipeline().parameters.WHERE_MASTER1099LISTMONTHLYSTAGE}
),
EXP_Master1099ListMonthlyStage AS (
	SELECT
	tax_id
	FROM SQ_Master1099ListMonthlyStage
),
SQ_Work1099Reporting AS (
	SELECT  Distinct Work1099Reporting.TaxId FROM Work1099Reporting
	Where Work1099Reporting.TaxIdType ='S' AND  @{pipeline().parameters.WHERE_WORK1099REPORTING}
	UNION
	SELECT  Distinct Work1099Reporting.SearchTaxId FROM Work1099Reporting
	Where Work1099Reporting.TaxIdType ='S' AND  @{pipeline().parameters.WHERE_WORK1099REPORTING}
),
EXP_Work1099Reporting AS (
	SELECT
	TaxId
	FROM SQ_Work1099Reporting
),
SQ_arch_client_tax_stage AS (
	SELECT Distinct arch_client_tax_stage.citx_tax_id 
	FROM
	 arch_client_tax_stage
	WHERE arch_client_tax_stage.tax_type_cd='SSN'
	AND @{pipeline().parameters.WHERE_ARCH_CLIENT_TAX_STAGE}
),
EXP_arch_client_tax_stage AS (
	SELECT
	citx_tax_id
	FROM SQ_arch_client_tax_stage
),
SQ_arch_master_1099_list_stage AS (
	SELECT Distinct arch_master_1099_list_stage.tax_id
	FROM
	 arch_master_1099_list_stage 
	WHERE arch_master_1099_list_stage.tax_id_type='S' AND @{pipeline().parameters.WHERE_ARCH_MASTER_1099_LIST_STAGE}
	UNION
	SELECT Distinct  arch_master_1099_list_stage.search_tax_id 
	FROM
	 arch_master_1099_list_stage 
	WHERE arch_master_1099_list_stage.tax_id_type='S' AND @{pipeline().parameters.WHERE_ARCH_MASTER_1099_LIST_STAGE}
),
EXP_arch_master_1099_list_stage AS (
	SELECT
	tax_id
	FROM SQ_arch_master_1099_list_stage
),
SQ_claim_master_1099_list_dim AS (
	SELECT Distinct claim_master_1099_list_dim.tax_id
	FROM
	 claim_master_1099_list_dim
	WHERE claim_master_1099_list_dim.tax_id_type='S' AND @{pipeline().parameters.WHERE_CLAIM_MASTER_1099_LIST_DIM}
	UNION
	SELECT Distinct claim_master_1099_list_dim.irs_tax_id 
	FROM
	 claim_master_1099_list_dim
	WHERE claim_master_1099_list_dim.tax_id_type='S' AND @{pipeline().parameters.WHERE_CLAIM_MASTER_1099_LIST_DIM}
),
EXP_claim_master_1099_list_dim AS (
	SELECT
	tax_id
	FROM SQ_claim_master_1099_list_dim
),
SQ_client_tax_stage AS (
	select Distinct  client_tax_stage.citx_tax_id from client_tax_stage
	WHERE client_tax_stage.tax_type_cd='SSN'
	AND @{pipeline().parameters.WHERE_CLIENT_TAX_STAGE}
),
EXP_client_tax_stage AS (
	SELECT
	citx_tax_id
	FROM SQ_client_tax_stage
),
SQ_master_1099_list_stage AS (
	SELECT  Distinct master_1099_list_stage.tax_id
	FROM
	 master_1099_list_stage
	where master_1099_list_stage.tax_id_type='S' AND  @{pipeline().parameters.WHERE_MASTER_1099_LIST_STAGE}
	UNION
	SELECT  Distinct master_1099_list_stage.search_tax_id 
	FROM
	 master_1099_list_stage
	where master_1099_list_stage.tax_id_type='S' AND @{pipeline().parameters.WHERE_MASTER_1099_LIST_STAGE}
),
EXP_master_1099_list_stage AS (
	SELECT
	tax_id
	FROM SQ_master_1099_list_stage
),
SQ_vendor_dba_1099_stage AS (
	SELECT Distinct vendor_dba_1099_stage.tax_id 
	FROM
	 vendor_dba_1099_stage
	@{pipeline().parameters.WHERE_VENDOR_DBA_1099_STAGE}
),
EXP_vendor_dba_1099_stage AS (
	SELECT
	tax_id
	FROM SQ_vendor_dba_1099_stage
),
SQ_claim_master_1099_list AS (
	SELECT Distinct claim_master_1099_list.tax_id FROM claim_master_1099_list
	WHERE claim_master_1099_list.tax_id_type='S' AND @{pipeline().parameters.WHERE_CLAIM_MASTER_1099_LIST}
	UNION
	SELECT Distinct claim_master_1099_list.irs_tax_id FROM claim_master_1099_list
	WHERE claim_master_1099_list.tax_id_type='S' AND @{pipeline().parameters.WHERE_CLAIM_MASTER_1099_LIST}
),
claim_master_1099_list AS (
	SELECT
	tax_id
	FROM SQ_claim_master_1099_list
),
Union_Input_Values_TaxIdTypes AS (
	SELECT tax_id
	FROM EXP_master_1099_list_stage
	UNION
	SELECT tax_id
	FROM EXP_claim_master_1099_list_dim
	UNION
	SELECT citx_tax_id AS tax_id
	FROM EXP_client_tax_stage
	UNION
	SELECT tax_id
	FROM EXP_Master1099ListMonthlyStage
	UNION
	SELECT TaxId AS tax_id
	FROM EXP_Work1099Reporting
	UNION
	SELECT tax_id
	FROM claim_master_1099_list
	UNION
	SELECT tax_id
	FROM EXP_arch_master_1099_list_stage
	UNION
	SELECT citx_tax_id AS tax_id
	FROM EXP_arch_client_tax_stage
	UNION
	SELECT dft_tax_id_nbr AS tax_id
	FROM EXP_ClaimDraftMonthlyStage
	UNION
	SELECT tax_id
	FROM EXP_vendor_dba_1099_stage
	UNION
	SELECT dft_tax_id_nbr AS tax_id
	FROM EXP_CLAIM_DRAFT_STAGE
	UNION
	SELECT dft_tax_id_nbr AS tax_id
	FROM EXP_ARCH_CLAIM_DRAFT_STAGE
),
EXP_SSN_FEIN_TAXID AS (
	SELECT
	tax_id AS id,
	tax_id AS ssn_fein_taxid,
	-- *INF*: LTRIM(RTRIM(ssn_fein_taxid))
	LTRIM(RTRIM(ssn_fein_taxid
		)
	) AS V_ssn_fein_taxid,
	-- *INF*: IIF(SUBSTR(V_ssn_fein_taxid,3,1)='-'OR (SUBSTR(V_ssn_fein_taxid,1,3)='000' AND (LENGTH(V_ssn_fein_taxid)=9 ))OR SUBSTR(V_ssn_fein_taxid,2,1)='-'  OR ISNULL(V_ssn_fein_taxid) OR (V_ssn_fein_taxid='N/A')   OR REG_MATCH(V_ssn_fein_taxid,'[*]*') OR(REG_MATCH(V_ssn_fein_taxid,'[\da-zA-Z]+') AND (LENGTH(V_ssn_fein_taxid)=11 OR LENGTH(V_ssn_fein_taxid)=10) )
	--  OR ((SUBSTR(V_ssn_fein_taxid,4,1)='-')  AND  (LENGTH(V_ssn_fein_taxid) != 11 )) OR (LENGTH(V_ssn_fein_taxid)<=6 ) OR  (LENGTH(V_ssn_fein_taxid)>11 ) ,'NOTVALID','VALID')
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(SUBSTR(V_ssn_fein_taxid, 3, 1
		) = '-' 
		OR ( SUBSTR(V_ssn_fein_taxid, 1, 3
			) = '000' 
			AND ( LENGTH(V_ssn_fein_taxid
				) = 9 
			) 
		) 
		OR SUBSTR(V_ssn_fein_taxid, 2, 1
		) = '-' 
		OR V_ssn_fein_taxid IS NULL 
		OR ( V_ssn_fein_taxid = 'N/A' 
		) 
		OR REGEXP_LIKE(V_ssn_fein_taxid, '[*]*'
		) 
		OR ( REGEXP_LIKE(V_ssn_fein_taxid, '[\da-zA-Z]+'
			) 
			AND ( LENGTH(V_ssn_fein_taxid
				) = 11 
				OR LENGTH(V_ssn_fein_taxid
				) = 10 
			) 
		) 
		OR ( ( SUBSTR(V_ssn_fein_taxid, 4, 1
				) = '-' 
			) 
			AND ( LENGTH(V_ssn_fein_taxid
				) != 11 
			) 
		) 
		OR ( LENGTH(V_ssn_fein_taxid
			) <= 6 
		) 
		OR ( LENGTH(V_ssn_fein_taxid
			) > 11 
		),
		'NOTVALID',
		'VALID'
	) AS V_flag,
	V_flag AS flag,
	-- *INF*: IIF(LENGTH(V_ssn_fein_taxid)>=7 AND LENGTH(V_ssn_fein_taxid)<=8,LPAD(V_ssn_fein_taxid,9,'0'),V_ssn_fein_taxid)
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	-- 
	IFF(LENGTH(V_ssn_fein_taxid
		) >= 7 
		AND LENGTH(V_ssn_fein_taxid
		) <= 8,
		LPAD(V_ssn_fein_taxid, 9, '0'
		),
		V_ssn_fein_taxid
	) AS V_taxid,
	-- *INF*: IIF(REG_MATCH(V_taxid,'[0-9-]*') ,V_taxid,'X')
	-- 
	-- 
	-- 
	IFF(REGEXP_LIKE(V_taxid, '[0-9-]*'
		),
		V_taxid,
		'X'
	) AS V_valid_taxid,
	V_valid_taxid AS flag_TaxId,
	-- *INF*: IIF(LENGTH(V_valid_taxid)=9  AND (REG_MATCH(V_valid_taxid,'^[0-9]*$'))  ,(SUBSTR(V_valid_taxid, 1, 3) ||'-'||SUBSTR(V_valid_taxid, 4, 2)||'-'||SUBSTR(V_valid_taxid, 6, 4)) ,V_valid_taxid)
	-- 
	-- 
	-- 
	-- 
	IFF(LENGTH(V_valid_taxid
		) = 9 
		AND ( REGEXP_LIKE(V_valid_taxid, '^[0-9]*$'
			) 
		),
		( SUBSTR(V_valid_taxid, 1, 3
			) || '-' || SUBSTR(V_valid_taxid, 4, 2
			) || '-' || SUBSTR(V_valid_taxid, 6, 4
			) 
		),
		V_valid_taxid
	) AS OUT_taxid
	FROM Union_Input_Values_TaxIdTypes
),
RTR_SSN_FEIN_TAXID AS (
	SELECT
	id,
	flag,
	ssn_fein_taxid AS fein_taxid,
	OUT_taxid AS ssn,
	flag_TaxId
	FROM EXP_SSN_FEIN_TAXID
),
RTR_SSN_FEIN_TAXID_NOTVALID_SSN AS (SELECT * FROM RTR_SSN_FEIN_TAXID WHERE flag = 'NOTVALID'),
RTR_SSN_FEIN_TAXID_VALID_SSN AS (SELECT * FROM RTR_SSN_FEIN_TAXID WHERE flag='VALID'  AND flag_TaxId != 'X'),
AGG_Removes_Duplicates AS (
	SELECT
	id AS o_ID,
	ssn AS adnm_taxid_ssn
	FROM RTR_SSN_FEIN_TAXID_VALID_SSN
	QUALIFY ROW_NUMBER() OVER (PARTITION BY o_ID, adnm_taxid_ssn ORDER BY NULL) = 1
),
Union_Inputs AS (
	SELECT o_ID, adnm_taxid_ssn
	FROM AGG_Remove_Duplicates
	UNION
	SELECT o_ID, adnm_taxid_ssn
	FROM AGG_Removes_Duplicates
),
EXP_Abort AS (
	SELECT
	o_ID,
	adnm_taxid_ssn,
	-- *INF*: IIF((SUBSTR(adnm_taxid_ssn,4,1)='-'),Abort ('Found Valid SSN Data and Aborting the job. Please tokenize the valid SSN data'))
	IFF(( SUBSTR(adnm_taxid_ssn, 4, 1
			) = '-' 
		),
		Abort('Found Valid SSN Data and Aborting the job. Please tokenize the valid SSN data'
		)
	) AS Abort
	FROM Union_Inputs
),
FIL_Abort AS (
	SELECT
	o_ID, 
	adnm_taxid_ssn, 
	Abort
	FROM EXP_Abort
	WHERE FALSE
),
Claims_OneTime_Conversion_Batch_File AS (
	INSERT INTO TEST_File
	(ID, SSN_TOKENS)
	SELECT 
	o_ID AS ID, 
	adnm_taxid_ssn AS SSN_TOKENS
	FROM FIL_Abort
),