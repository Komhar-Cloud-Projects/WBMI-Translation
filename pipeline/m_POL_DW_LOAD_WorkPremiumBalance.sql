WITH
SQ_arch_pif_4514_stage AS (
	SELECT 
	        LTRIM(RTRIM(arch.pif_symbol)),
	       arch.pif_policy_number,
	       arch.pif_module,
	       arch.sar_id,
	       LTRIM(RTRIM(arch.sar_insurance_line)),
	       LTRIM(RTRIM(arch.sar_location_x)),
	       LTRIM(RTRIM(arch.sar_sub_location_x)),
	       LTRIM(RTRIM(arch.sar_risk_unit_group)),
	       LTRIM(RTRIM(arch.sar_class_code_grp_x)),
	       LTRIM(RTRIM(arch.sar_class_code_mem_x)),
	       LTRIM(RTRIM(arch.sar_unit)),
	       LTRIM(RTRIM(arch.sar_risk_unit_continued)),
	       LTRIM(RTRIM(arch.sar_seq_rsk_unt_a)),
	       LTRIM(RTRIM(arch.sar_major_peril)),
	       LTRIM(RTRIM(arch.sar_seq_no)),
	       arch.sar_cov_eff_year,
	       arch.sar_cov_eff_month,
	       arch.sar_cov_eff_day,
	       sar_part_code,
	       LTRIM(RTRIM(arch.sar_entrd_date)),
	       arch.sar_transaction,
	       arch.sar_premium,
	       arch.sar_original_prem,
	       arch.sar_agents_comm_rate,
	       arch.sar_acct_entrd_date,
	       LTRIM(RTRIM(arch.sar_state)),
	       arch.sar_rsn_amend_one,
	       arch.sar_rsn_amend_two,
	       arch.sar_rsn_amend_three,
	       arch.sar_special_use,
	       arch.sar_type_bureau
	FROM   @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_ARCH} arch
	INNER JOIN
	(SELECT pif_symbol,
	pif_policy_number,
	pif_module,
	MAX(audit_id) AS audit_id
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_ARCH}                  
	WHERE  logical_flag IN ('0','1','2','3')  
	AND sar_acct_entrd_date=CONVERT(char(6), DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTH},0),112) 
	AND @{pipeline().parameters.WHERE_CLAUSE_ARCH}            
	GROUP  BY pif_symbol,pif_policy_number,pif_module) A
	ON arch.pif_symbol=A.pif_symbol
	AND arch.pif_policy_number=A.pif_policy_number
	AND arch.pif_module=A.pif_module
	AND arch.audit_id=A.audit_id
	WHERE arch.logical_flag IN ('0','1','2','3')  
	AND arch.sar_acct_entrd_date=CONVERT(char(6), DATEADD(MONTH,DATEDIFF(MONTH,0,GETDATE())-@{pipeline().parameters.NO_OF_MONTH},0),112) 
	AND @{pipeline().parameters.WHERE_CLAUSE_2}
),
EXP_Default_Archive AS (
	SELECT
	pif_symbol AS arch_pif_symbol,
	pif_policy_number AS arch_pif_policy_number,
	pif_module AS arch_pif_module,
	sar_id AS arch_sar_id,
	sar_insurance_line AS arch_sar_insurance_line,
	sar_location_x AS arch_sar_location_x,
	sar_sub_location_x AS arch_sar_sub_location_x,
	sar_risk_unit_group AS arch_sar_risk_unit_group,
	sar_class_code_grp_x AS arch_sar_class_code_grp_x,
	sar_class_code_mem_x AS arch_sar_class_code_mem_x,
	sar_unit AS arch_sar_unit,
	sar_risk_unit_continued AS arch_sar_risk_unit_continued,
	sar_seq_rsk_unt_a AS arch_sar_seq_rsk_unt_a,
	sar_major_peril AS arch_sar_major_peril,
	sar_seq_no AS arch_sar_seq_no,
	sar_cov_eff_year AS arch_sar_cov_eff_year,
	sar_cov_eff_month AS arch_sar_cov_eff_month,
	sar_cov_eff_day AS arch_sar_cov_eff_day,
	sar_part_code AS arch_sar_part_code,
	sar_entrd_date AS arch_sar_entrd_date,
	sar_transaction AS arch_sar_transaction,
	sar_premium AS arch_sar_premium,
	sar_original_prem AS arch_sar_original_prem,
	sar_agents_comm_rate AS arch_sar_agents_comm_rate,
	sar_acct_entrd_date AS arch_sar_acct_entrd_date,
	sar_state AS arch_sar_state,
	sar_rsn_amend_one AS arch_sar_rsn_amend_one,
	sar_rsn_amend_two AS arch_sar_rsn_amend_two,
	sar_rsn_amend_three AS arch_sar_rsn_amend_three,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(arch_sar_rsn_amend_one  ||  arch_sar_rsn_amend_two || arch_sar_rsn_amend_three)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(arch_sar_rsn_amend_one || arch_sar_rsn_amend_two || arch_sar_rsn_amend_three
	) AS o_ReasonAmendedCode,
	sar_special_use AS arch_sar_special_use,
	sar_type_bureau AS arch_sar_type_bureau
	FROM SQ_arch_pif_4514_stage
),
EXP_Evaluate AS (
	SELECT
	arch_pif_symbol AS i_arch_pif_symbol,
	arch_pif_policy_number AS i_arch_pif_policy_number,
	arch_pif_module AS i_arch_pif_module,
	arch_sar_major_peril,
	arch_sar_part_code,
	arch_sar_entrd_date,
	arch_sar_transaction,
	arch_sar_premium,
	arch_sar_original_prem,
	arch_sar_agents_comm_rate,
	arch_sar_acct_entrd_date,
	arch_sar_state,
	o_ReasonAmendedCode AS ReasonAmendedCode,
	arch_sar_special_use,
	arch_sar_type_bureau,
	i_arch_pif_symbol  ||  i_arch_pif_policy_number  || i_arch_pif_module AS o_PolicyKey
	FROM EXP_Default_Archive
),
EXP_Values AS (
	SELECT
	arch_sar_major_peril AS i_arch_sar_major_peril,
	arch_sar_part_code AS i_arch_sar_part_code,
	arch_sar_entrd_date AS i_arch_sar_entrd_date,
	arch_sar_transaction AS i_arch_sar_transaction,
	arch_sar_premium AS i_arch_sar_premium,
	arch_sar_original_prem AS i_arch_sar_original_prem,
	arch_sar_agents_comm_rate AS i_arch_sar_agents_comm_rate,
	arch_sar_acct_entrd_date AS i_arch_sar_acct_entrd_date,
	arch_sar_state AS i_arch_sar_state,
	ReasonAmendedCode AS i_ReasonAmendedCode,
	arch_sar_special_use AS i_arch_sar_special_use,
	arch_sar_type_bureau AS i_arch_sar_type_bureau,
	o_PolicyKey AS i_PolicyKey,
	SYSDATE AS o_ExtractDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemId,
	i_PolicyKey AS o_PolicyKey,
	i_arch_sar_major_peril AS o_SarMajorPerilCode,
	i_arch_sar_part_code AS o_SarPartCode,
	i_arch_sar_entrd_date AS o_SarEnteredDate,
	i_arch_sar_transaction AS o_SarTransaction,
	i_arch_sar_premium AS o_SarPremium,
	i_arch_sar_original_prem AS o_SarOriginalPremium,
	i_arch_sar_agents_comm_rate AS o_SarAgentCommissionRate,
	i_arch_sar_acct_entrd_date AS o_SarAccountEnteredDate,
	i_arch_sar_state AS o_SarState,
	i_ReasonAmendedCode AS o_ReasonAmendedCode,
	i_arch_sar_special_use AS o_SarSpecialUse,
	i_arch_sar_type_bureau AS o_SarTypeBureau
	FROM EXP_Evaluate
),
WorkPremiumBalance AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumBalance;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumBalance
	(ExtractDate, SourceSystemId, PolicyKey, SarMajorPerilCode, SarPartCode, SarEnteredDate, SarTransaction, SarPremium, SarOriginalPremium, SarAgentCommissionRate, SarAccountEnteredDate, SarState, ReasonAmendedCode, SarSpecialUse, SarTypeBureau)
	SELECT 
	o_ExtractDate AS EXTRACTDATE, 
	o_SourceSystemId AS SOURCESYSTEMID, 
	o_PolicyKey AS POLICYKEY, 
	o_SarMajorPerilCode AS SARMAJORPERILCODE, 
	o_SarPartCode AS SARPARTCODE, 
	o_SarEnteredDate AS SARENTEREDDATE, 
	o_SarTransaction AS SARTRANSACTION, 
	o_SarPremium AS SARPREMIUM, 
	o_SarOriginalPremium AS SARORIGINALPREMIUM, 
	o_SarAgentCommissionRate AS SARAGENTCOMMISSIONRATE, 
	o_SarAccountEnteredDate AS SARACCOUNTENTEREDDATE, 
	o_SarState AS SARSTATE, 
	o_ReasonAmendedCode AS REASONAMENDEDCODE, 
	o_SarSpecialUse AS SARSPECIALUSE, 
	o_SarTypeBureau AS SARTYPEBUREAU
	FROM EXP_Values
),