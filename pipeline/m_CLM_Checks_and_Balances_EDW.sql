WITH
SQ_curr_row_count AS (
	-- Claim_Party Table
	SELECT count(*) as EDW_count,'Count of Claim_Party_ak_id from Claim_Party with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Party' as target_name
	FROM (SELECT Claim_Party_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Party
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY Claim_Party_ak_id
	             HAVING (COUNT(*) > 1)) CP
	
	UNION 
	-- Claim_Occurrence Table
	SELECT count(*) as EDW_count,'Count of Claim_Occurrence_ak_id from Claim_Occurrence with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Occurrence' as target_name
	FROM (SELECT Claim_occurrence_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY Claim_occurrence_ak_id
	             HAVING (COUNT(*) > 1)) CO
	
	UNION
	-- Claim_Party_Occurrence Table
	SELECT count(*) as EDW_count,'Count of Claim_Party_Occurrence_ak_id from Claim_Party_Occurrence with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Party_Cccurrence' as target_name
	FROM (SELECT Claim_Party_Occurrence_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Party_Occurrence
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY Claim_Party_Occurrence_ak_id
	             HAVING (COUNT(*) > 1)) CPO
	
	UNION
	-- Claim Case Table
	SELECT count(*) as EDW_count,'Count of Claim_Case_Ak_id from claim_case with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message, 'Claim_Case' as target_name FROM (SELECT Claim_Case_Ak_id, COUNT(*) AS Expr1
	FROM   dbo.Claim_Case
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY Claim_case_ak_id
	             HAVING (COUNT(*) > 1)) CCase
	
	UNION
	-- Claim_Party_Relation Table
	SELECT count(*) as EDW_count,'Count of Claim_Party_Relation_ak_id from Claim_Party_Relation with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Party_Relation' as target_name
	FROM (SELECT Claim_Party_Relation_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Party_Relation
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY Claim_Party_Relation_ak_id
	             HAVING (COUNT(*) > 1)) CPR
	
	UNION
	-- Claim_Representative Table 
	SELECT count(*) as EDW_count,'Count of Claim_Rep_ak_id from Claim_Representative with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message, 'Claim_Representative'  as target_name
	FROM (SELECT Claim_Rep_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Representative
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY Claim_Rep_ak_id
	             HAVING (COUNT(*) > 1)) CR
	
	UNION
	-- Claim_Representative_Occurrence Table
	SELECT count(*) as EDW_count,'Count of Claim_Rep_Occurrence_ak_id from Claim_Representative_Occurrence with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message, 'Claim_Representative_Occurrence' as target_name
	FROM (SELECT Claim_Rep_Occurrence_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Representative_Occurrence
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY Claim_Rep_Occurrence_ak_id
	             HAVING (COUNT(*) > 1)) CRO
	
	UNION
	-- Claimant_Coverage_Detail Table
	SELECT count(*) as EDW_count,'Count of Claimant_Cov_det_ak_id from Claimant_Coverage_Detail with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claimant_Coverage_detail' as target_name
	FROM (SELECT Claimant_Cov_det_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Coverage_Detail
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY Claimant_Cov_det_ak_id
	             HAVING (COUNT(*) > 1)) CCD
	
	UNION
	-- Workers_Comp_Claimant_Detail Table
	SELECT count(*) as EDW_count,'Count of claim_party_occurrence_ak_id from Workers_Comp_Claimant_Detail with more than one record with 
	crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message, 'Workers_Comp_Claimant_Detail' as target_name
	FROM (SELECT claim_party_occurrence_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Workers_Comp_Claimant_Detail
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY claim_party_occurrence_ak_id
	             HAVING (COUNT(*) > 1)) WCCD
	
	UNION
	-- Claim_Transaction Table
	SELECT count(*) as EDW_count,'Count of Claim_Trans_ak_id from Claim_Transaction with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message, 'Claim_Transaction' as target_name
	FROM (SELECT Claim_Trans_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Transaction
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY Claim_Trans_ak_id
	             HAVING (COUNT(*) > 1)) CT
	
	UNION
	-- Claim_Payment Table
	SELECT count(*) as EDW_count,'Count of Claim_Pay_ak_id from Claim_Payment with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Payment' as target_name
	FROM (SELECT claim_pay_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Payment
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY claim_pay_ak_id
	             HAVING (COUNT(*) > 1)) CPay
	
	UNION
	-- Claimant_Coverage_Detail_Reserve_Calculation Table
	SELECT count(*) as EDW_count,'Count of Claimant_Cov_det_ak_id from Claimant_Coverage_Detail_Reserve_Calculation with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claimant_Coverage_Detail_Reserve_Calculation' as target_name
	FROM (SELECT claimant_cov_det_ak_id,financial_type_code, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Coverage_Detail_Reserve_Calculation
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY claimant_cov_det_ak_id,financial_type_code
	             HAVING (COUNT(*) > 1)) CCDRC
	
	UNION
	-- Claimant_Coverage_Detail_Calculation Table
	SELECT count(*) as EDW_count,'Count of Claimant_Cov_Det_ak_id from Claimant_Coverage_Detail_Calculation with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claimant_Coverage_Detail_Calculation' as target_name
	FROM (SELECT claimant_cov_det_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Coverage_Detail_Calculation
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY claimant_cov_det_ak_id
	             HAVING (COUNT(*) > 1)) CCDC
	
	UNION
	-- Claimant_Reserve_Calculation Table
	SELECT count(*) as EDW_count,'Count of Claim_Party_Occurrence_ak_id from Claimant_Reserve_Calculation with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claimant_Reserve_Calculation' as target_name
	FROM (SELECT claim_party_occurrence_ak_id,financial_type_code, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Reserve_Calculation
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY claim_party_occurrence_ak_id,financial_type_code
	             HAVING (COUNT(*) > 1)) CRC
	
	UNION
	-- Claimant_Calculation Table
	SELECT count(*) as EDW_count,'Count of Claim_Party_Occurrence_ak_id from Claimant_Calculation with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claimant_Calculation' as target_name
	FROM (SELECT claim_party_occurrence_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Calculation
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY claim_party_occurrence_ak_id
	             HAVING (COUNT(*) > 1)) CC
	
	UNION
	-- Claim_Occurrence_Reserve_Calculation Table
	SELECT count(*) as EDW_count,'Count of Claim_Occurrence_ak_id from Claim_Occurrence_Reserve_Calculation with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Occurrence_Reserve_Calculation' as target_name
	FROM (SELECT claim_occurrence_ak_id,financial_type_code, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence_Reserve_Calculation 
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY claim_occurrence_ak_id,financial_type_code
	             HAVING (COUNT(*) > 1)) CORC
	
	UNION
	-- Claim_Occurrence_Calculation Table
	SELECT count(*) as EDW_count,'Count of Claim_Occurrence_ak_id from Claim_Occurrence_Calculation with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message,'Claim_Occurrence_Calculation' as target_name
	FROM (SELECT claim_occurrence_ak_id, COUNT(*) AS Expr1
	             FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence_Calculation
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY claim_occurrence_ak_id
	             HAVING (COUNT(*) > 1)) COC
	
	UNION
	--- Claim Payment Category Table
	SELECT count(*) as EDW_count,'Count of Claim_Pay_Ctgry_Ak_id from Claim_Payment_Category with more than one record with crrnt snpsht flag 1 = ' 
	+ convert(varchar,count(*)) as check_out_message, 'Claim_Payment_Category' as target_name
	FROM (SELECT claim_pay_ctgry_ak_id, COUNT(*) AS Expr1
	             FROM   dbo.Claim_Payment_Category
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY claim_pay_ctgry_ak_id
	             HAVING (COUNT(*) > 1)) CPC
	
	UNION
	-- Claim Reinsurance Transaction Table
	SELECT count(*) as EDW_count,'Count of Claim_Reins_Trans_ak_id from Claim_Reinsurance_Transaction with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message, 'Claim_Transaction' as target_name
	FROM (SELECT Claim_Reins_Trans_ak_id, COUNT(*) AS Expr1
	             FROM   dbo.Claim_Reinsurance_Transaction
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY Claim_Reins_Trans_ak_id
	             HAVING (COUNT(*) > 1)) CT
	
	UNION
	-- Claim Party Occurrence Payment Table
	SELECT count(*) as EDW_count,'Count of Claim_Party_Occurrence_Pay_ak_id from Claim_Party_Occurrence_Payment with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message, 'Claim_Party_Occurrence_Payment' as target_name
	FROM (SELECT claim_party_occurrence_pay_ak_id, COUNT(*) AS Expr1
	             FROM   dbo.claim_party_occurrence_payment
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY claim_party_occurrence_pay_ak_id
	             HAVING (COUNT(*) > 1)) CPOP
	
	UNION
	--Policy Table
	SELECT count(*) as EDW_count,'Count of Pol_Ak_id from Policy with more than one record with crrnt snpsht flag 1 = ' + convert(varchar,count(*)) as check_out_message, 'Policy' as target_name 
	FROM (SELECT Pol_Ak_id, COUNT(*) AS Expr1
	FROM   V2.Policy
	             WHERE  (crrnt_snpsht_flag = 1)
	             GROUP BY Pol_Ak_id
	             HAVING (COUNT(*) > 1)) Policykey
	
	UNION
	----Count of Pol_ak_id from Policy where there are different Pol_key for same Pol_ak_id
	SELECT count(*) as EDW_count,'Count of Pol_Ak_id from Policy with different pol_key for same Ak_id = ' + convert(varchar,count(*)) as check_out_message,'Policy' as target_name
	FROM (select pol_ak_id,count(pol_key) as EXp1 from V2.policy 
	      where crrnt_snpsht_flag =1 
	      group by pol_ak_id
	      having count(pol_key) > 1) PK
	
	UNION
	--- Count of Claim_Occurrence_ak_id from Claim_Occurrence with different Claim_Occurrence_key for same Ak_id
	SELECT count(*) as EDW_count,'Count of Claim_Occurrence_ak_id from Claim_Occurrence with different Claim_Occurrence_key for same Ak_id = ' + convert(varchar,count(*)) as check_out_message,'Claim_Party_Cccurrence' as target_name
	FROM (SELECT Claim_Occurrence_ak_id,count(Claim_Occurrence_key) as Exp1 FROM DBO.Claim_Occurrence 
	                WHERE crrnt_snpsht_flag =1 
	                GROUP BY Claim_Occurrence_ak_id
	                HAVING count(Claim_Occurrence_key) > 1) CO1
	
	UNION
	---- Count of Claim_Party_ak_id from Claim_Party with different Party_key for same Ak_id
	SELECT count(*) as EDW_count,'Count of Claim_Party_ak_id from Claim_Party with different Claim_Party_key for same Ak_id = ' + convert(varchar,count(*)) as check_out_message,'Claim_Party_Cccurrence' as target_name
	FROM (SELECT Claim_Party_ak_id,count(Claim_Party_key) as Exp1 from DBO.Claim_Party 
	      WHERE crrnt_snpsht_flag =1 
	      GROUP BY  Claim_Party_ak_id
	      HAVING count(Claim_Party_key) > 1) CP1
	
	UNION
	---- Count of Claim_Case_Ak_id from Claim_Case with different Claim_Case_key for same ak_id
	SELECT count(*) as EDW_count,'Count of Claim_Case_ak_id from Claim_Case with different Claim_Case_key for same Ak_id = ' + convert(varchar,count(*)) as check_out_message,'Claim_Party_Cccurrence' as target_name
	FROM (SELECT Claim_Case_ak_id,count(Claim_Case_key) as Exp1 from DBO.Claim_Case
	      WHERE crrnt_snpsht_flag =1 
	      GROUP BY  Claim_Case_ak_id
	      HAVING count(Claim_Case_key) > 1) CCase
),
EXP_default AS (
	SELECT
	EDW_count,
	check_out_message,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS wbmi_session_control_run_id,
	'InformS' AS created_user_id,
	SYSDATE AS created_date,
	'InformS' AS modified_user_id,
	SYSDATE AS modified_date,
	target_name,
	-- *INF*: 'E'
	-- 
	-- //E - Error, W - Warning
	'E' AS checkout_type_code
	FROM SQ_curr_row_count
),
FILTRANS AS (
	SELECT
	EDW_count, 
	check_out_message, 
	wbmi_session_control_run_id, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date, 
	target_name, 
	checkout_type_code
	FROM EXP_default
	WHERE EDW_count>0
),
wbmi_checkout_curr_row_count AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID, 
	CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	EDW_count AS TARGET_COUNT, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM FILTRANS
),
SQ_Claim_Occurrence_not_in_CO_Calc AS (
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Occurrence_ak_id from Claim_Occurrence table where Claim_Occurrence_ak_id not in Claim_Occurrence_Calculation id = ' + convert(varchar,count(*)) as Check_Out_Message, 'Claim_Occurrence' as Target_Name FROM 
	(SELECT claim_occurrence_ak_id  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence WHERE  
	claim_occurrence_ak_id NOT IN  (SELECT claim_occurrence_ak_id  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence_Calculation))COC
	
	UNION
	
	SELECT COUNT(*) as EDW_Count, 'Count of Claim_Party_Occurrence_ak_id from Claim_Party_Occurrence table where the Claim_Occurrence_ak_id = 0 is ' + CONVERT(varchar,COUNT(*))
	 as Check_Out_Message, 'Claim_Party_Occurrence' as Target_Name  FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Party_Occurrence WHERE claim_occurrence_ak_id = 0
	
	UNION
	
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Party_ak_id from Claim_party table where Claim_party_ak_id have no continuity in eff_from_date & eff_to_date = ' 
	+ convert(varchar,count(*)) as Check_Out_Message, 'Claim_Party' as Target_Name FROM 
	(SELECT claim_party_ak_id FROM claim_party a WHERE not exists (SELECT 1 FROM Claim_Party b where a.Claim_Party_ak_id = b.Claim_Party_ak_id and 
	b.eff_from_date = dateadd(ss,1,a.eff_to_date) )
	and (a.eff_to_date <> '2100-12-31 23:59:59.000'))CP
	
	UNION
	
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Occurrence_ak_id from Claim_Occurrence table where Claim_Occurrence_ak_id have no continuity in eff_from_date & eff_to_date = ' 
	+ convert(varchar,count(*)) as Check_Out_Message, 'Claim_Occurrence' as Target_Name FROM 
	(SELECT Claim_Occurrence_ak_id FROM Claim_Occurrence a WHERE not exists (SELECT 1 FROM Claim_Occurrence b where a.Claim_Occurrence_ak_id = b.Claim_Occurrence_ak_id and 
	b.eff_from_date = dateadd(ss,1,a.eff_to_date) )
	and (a.eff_to_date <> '2100-12-31 23:59:59.000'))CO
	
	UNION
	
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Rep_ak_id from Claim_Representative table where Claim_Rep_ak_id have no continuity in eff_from_date & eff_to_date = ' 
	+ convert(varchar,count(*)) as Check_Out_Message, 'Claim_Representative' as Target_Name FROM 
	(SELECT Claim_Rep_ak_id FROM Claim_Representative a WHERE not exists (SELECT 1 FROM Claim_Representative b where a.Claim_Rep_ak_id = b.Claim_Rep_ak_id and 
	b.eff_from_date = dateadd(ss,1,a.eff_to_date) )
	and ( a.eff_to_date <> '2100-12-31 23:59:59.000'))CR
	
	UNION
	
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Rep_Occurrence_ak_id from Claim_Representative_Occurrence table where Claim_Rep_Occurrence_ak_id have no continuity in eff_from_date & eff_to_date = ' 
	+ convert(varchar,count(*)) as Check_Out_Message, 'Claim_Representative_Occurrence' as Target_Name FROM 
	(SELECT Claim_Rep_Occurrence_ak_id FROM Claim_Representative_Occurrence a WHERE not exists (SELECT 1 FROM Claim_Representative_Occurrence b where a.Claim_Rep_Occurrence_ak_id = b.Claim_Rep_Occurrence_ak_id and 
	b.eff_from_date = dateadd(ss,1,a.eff_to_date) )
	and ( a.eff_to_date <> '2100-12-31 23:59:59.000'))CRO
	
	UNION
	
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Party_Occurrence_ak_id from Claim_Party_Occurrence table where Claim_Party_Occurrence_ak_id have no continuity in eff_from_date & eff_to_date = ' 
	+ convert(varchar,count(*)) as Check_Out_Message, 'Claim_Party_Occurrence' as Target_Name FROM 
	(SELECT Claim_Party_Occurrence_ak_id FROM Claim_Party_Occurrence a WHERE not exists (SELECT 1 FROM Claim_Party_Occurrence b where a.Claim_Party_Occurrence_ak_id = b.Claim_Party_Occurrence_ak_id and 
	b.eff_from_date = dateadd(ss,1,a.eff_to_date) )
	and (a.eff_to_date <> '2100-12-31 23:59:59.000'))CPO
	
	UNION
	
	SELECT COUNT(*) as EDW_count, 'Count of Claimant_Cov_Det_ak_id from Claimant_Coverage_Detail table where Claimant_Cov_Det_ak_id have no continuity in eff_from_date & eff_to_date = ' 
	+ convert(varchar,count(*)) as Check_Out_Message, 'Claimant_Coverage_Detail' as Target_Name FROM 
	(SELECT Claimant_Cov_Det_ak_id FROM Claimant_Coverage_Detail a WHERE not exists (SELECT 1 FROM Claimant_Coverage_Detail b where a.Claimant_Cov_Det_ak_id = b.Claimant_Cov_Det_ak_id and 
	b.eff_from_date = dateadd(ss,1,a.eff_to_date) )
	and ( a.eff_to_date <> '2100-12-31 23:59:59.000'))CCD
	
	UNION
	
	SELECT COUNT(*) as EDW_count, 'Count of WC_Claimant_Det_ak_id from Workers_Comp_Claimant_Detail table where WC_Claimant_Det_ak_id have no continuity in eff_from_date & eff_to_date = ' 
	+ convert(varchar,count(*)) as Check_Out_Message, 'Workers_Comp_Claimant_Detail' as Target_Name FROM 
	(SELECT WC_Claimant_Det_ak_id FROM Workers_Comp_Claimant_Detail a WHERE not exists (SELECT 1 FROM Workers_Comp_Claimant_Detail b where a.WC_Claimant_Det_ak_id = b.WC_Claimant_Det_ak_id and 
	b.eff_from_date = dateadd(ss,1,a.eff_to_date) )
	and ( a.eff_to_date <> '2100-12-31 23:59:59.000'))WCD
),
EXP_default4 AS (
	SELECT
	EDW_count,
	Check_Out_Message AS check_out_message,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS wbmi_session_control_run_id,
	'InformS' AS created_user_id,
	SYSDATE AS created_date,
	'InformS' AS modified_user_id,
	SYSDATE AS modified_date,
	Target_Name AS target_name,
	-- *INF*: 'E'
	-- 
	-- //E - Error, W - Warning
	'E' AS checkout_type_code
	FROM SQ_Claim_Occurrence_not_in_CO_Calc
),
FILTRANS4 AS (
	SELECT
	EDW_count, 
	check_out_message, 
	wbmi_session_control_run_id, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date, 
	target_name, 
	checkout_type_code
	FROM EXP_default4
	WHERE EDW_count>0
),
wbmi_checkout_Claim_Occurrence_not_in_CO_Calc AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID, 
	CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	EDW_count AS TARGET_COUNT, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM FILTRANS4
),
SQ_Policy_key AS (
	-- Policy Key table
	SELECT COUNT(*) AS EDW_Count, 'Count of Pol_ak_id in Policy table where Policy is balancing without its claims balancing = ' + CONVERT(varchar,count(*)) as Check_Out_Message,'Policy' as Target_Name
	FROM (SELECT pol_key FROM V2.policy WHERE err_flag_bal_txn = '1'and crrnt_snpsht_flag = '1' AND pol_key IN (
	SELECT distinct pol_key FROM Claim_Occurrence WHERE err_flag_bal_txn = - 1 and source_sys_id = 'PMS' and crrnt_snpsht_flag = '1')) pol
	
	UNION
	-- Counts of Policy where policies are not matching for normal Claim Transactions
	SELECT COUNT(*) as EDW_count, 'Count of Policy_ak_id from V2.Policy table where Policy (Claim Txns) doesnt balance between Stage and EDW = ' + CONVERT(varchar,count(*)) as check_out_message,'Policy' as target_name
	FROM (select Pol_ak_id
		from V2.policy
		WHERE err_flag_bal_txn = -1 and crrnt_snpsht_flag = 1)PK
	
	UNION
	-- Counts of Claims where Claims are not matching for normal Claim Transactions
	SELECT COUNT(*) as EDW_count, 'Count of PMS Claim_Occurrence_ak_id from Claim_Occurrence table where Claim Transaction Amt doesnt balance between Stage and EDW = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Occurrence' as target_name
	FROM (select claim_occurrence_ak_id
		from dbo.claim_occurrence
		WHERE err_flag_bal_txn = -1 and crrnt_snpsht_flag = 1)CO
	
	UNION
	-- Counts of Claims where Claims are not matching for normal Claim Transactions
	SELECT COUNT(*) as EDW_count, 'Count of PMS Claim_Occurrence_ak_id from Claim_Occurrence table where Claim Transaction Rows doesnt balance between Stage and EDW = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Occurrence' as target_name
	FROM (select claim_occurrence_ak_id
		from dbo.claim_occurrence
		WHERE err_flag_bal_txn = -2 and crrnt_snpsht_flag = 1)CO
	
	UNION
	-- Counts of Claims where Claims are not matching for normal Claim Transactions
	SELECT COUNT(*) as EDW_count, '_Count of EXCEED Claim_Occurrence_ak_id from Claim_Occurrence table where Claim Transaction Amt doesnt balance between Stage and EDW = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Occurrence' as target_name
	FROM (select claim_occurrence_ak_id
		from dbo.claim_occurrence
		WHERE err_flag_bal_txn = -3 and crrnt_snpsht_flag = 1)CO
	
	UNION
	-- Counts of Claims where Claims are not matching for normal Claim Transactions
	SELECT COUNT(*) as EDW_count, '_Count of EXCEED Claim_Occurrence_ak_id from Claim_Occurrence table where Claim Transaction Rows doesnt balance between Stage and EDW = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Occurrence' as target_name
	FROM (select claim_occurrence_ak_id
		from dbo.claim_occurrence
		WHERE err_flag_bal_txn = -4 and crrnt_snpsht_flag = 1)CO
	
	UNION
	-- Counts of Policy where policies are not matching for  Claim Reinsurance Transactions
	SELECT COUNT(*) as EDW_count, 'Count of Policy_ak_id from Policy table where Policy (Claim Reins Txns) doesnt balance between Stage and EDW = ' + CONVERT(varchar,count(*)) as check_out_message,'Policy' as target_name
	FROM (select Pol_ak_id
		from v2.policy
		WHERE err_flag_bal_reins = -1 and crrnt_snpsht_flag = 1)PK
	
	UNION
	-- Counts of Claims where Claims are not matching for Claim Reinsurance Transactions
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Occurrence_ak_id from Claim_Occurrence table where Claim Reins Txns doesnt balance between Stage and EDW = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Occurrence' as target_name
	FROM (select claim_occurrence_ak_id
		from dbo.claim_occurrence
		WHERE err_flag_bal_reins = -1 and crrnt_snpsht_flag = 1)CO
	
	--commneted for June 2010 release as this SQL started running much longer. This can be uncommented 
	--when this runs within acceptable time
	
	--UNION
	---- Counts of Claim Case where it doesnt have a Claimant in Claim Party Occurrence
	--SELECT COUNT(*) as EDW_count, 'Count of Claim_Case_ak_id from Claim_Case table where Claim Case doesnt have a claimant in Claim_Party_Occurrence = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Case' as target_name
	--FROM (SELECT distinct Claim_Case_Ak_id
	--	from dbo.Claim_Case
	--	WHERE claim_case_ak_id not in (SELECT distinct claim_case_ak_id FROM Claim_Party_Occurrence))CCase
	
	UNION
	--- Count of Transactions where sar_id > 94
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Trans_ak_id from Claim_Transaction table where sar_id > 94 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Occurrence' as target_name
	FROM (SELECT claim_trans_id FROM dbo.claim_transaction
		WHERE sar_id > '94' and sar_id <> 'N/A' and crrnt_snpsht_flag = 1)CTS
	
	UNION
	--- Count of payments where pay_issued_date > pay_cashed_date
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Pay_ak_id from Claim_Payment table where pay_issued_date > pay_cashed_date = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Payment' as target_name
	FROM (SELECT claim_pay_ak_id FROM dbo.claim_payment
	WHERE pay_issued_date > pay_cashed_date and pay_cashed_date != '01/01/1800 01:00:00' and pay_cashed_date != '01/01/1800 00:00:00' )CP
	
	UNION
	--count rows from Claim_transaction with Sar_id of N/A
	select COUNT(*) as EDW_count,
	'Count of Claim Transaction joined to claimant_coverage_detail with Sar_id of N/A and Major Peril not 101 =  ' +CONVERT(varchar,Count(*)) as check_out_message,
	'Claim_Transaction and Claimant_Coverage_Detail' as target_name 
	FROM  vw_claim_transaction CT, claimant_coverage_detail CCD where sar_id='N/A' and CT.claimant_cov_det_ak_id = CCD.claimant_cov_det_ak_id and major_peril_code <> '101'
	
	UNION
	
	--count rows from Claim_transaction with Sar_id of N/A
	select COUNT(*) as EDW_count,
	'Count of Claim Transaction joined to claimant_coverage_detail with Sar_id of N/A and Major Peril = 101 =  ' +CONVERT(varchar,Count(*)) as check_out_message,
	'Claim_Transaction and Claimant_Coverage_Detail' as target_name 
	FROM  vw_claim_transaction CT, claimant_coverage_detail CCD where sar_id='N/A' and CT.claimant_cov_det_ak_id = CCD.claimant_cov_det_ak_id and major_peril_code = '101'
	
	UNION
	
	--count rows from Claimant_Coverage_Detail where pmsTypeBureau=N/A
	select COUNT(*) as EDW_count,
	'Count of Claimant_Coverage_Detail with pms type bureau of N/A and major peril NOT 101= ' + convert(varchar,count(*)) as check_out_message,
	'Claimant_Coverage_Detail' as target_name 
	FROM  claimant_coverage_detail CCD 
	where CCD.pms_type_bureau_code='N/A' and CCD.major_peril_code<>'101'
),
EXP_default41 AS (
	SELECT
	EDW_count,
	Check_Out_Message AS check_out_message,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS wbmi_session_control_run_id,
	'InformS' AS created_user_id,
	SYSDATE AS created_date,
	'InformS' AS modified_user_id,
	SYSDATE AS modified_date,
	Target_Name AS target_name,
	-- *INF*: 'W'
	-- 
	-- //E - Error, W - Warning
	'W' AS checkout_type_code
	FROM SQ_Policy_key
),
FILTRANS41 AS (
	SELECT
	EDW_count, 
	check_out_message, 
	wbmi_session_control_run_id, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date, 
	target_name, 
	checkout_type_code
	FROM EXP_default41
	WHERE EDW_count>0
),
wbmi_checkout_Policy_key AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID, 
	CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	EDW_count AS TARGET_COUNT, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM FILTRANS41
),
SQ_EDW_curr_row_AND_eff_from_date AS (
	--- Policy Table 
	SELECT COUNT(*) as EDW_count, 'Count of Pol_ak_id in Policy table with crrnt_snpsht_flag = 1 that 
	has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Policy' as target_name 
	from V2.Policy where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000' 
	
	--- Claim_Occurrence Table 
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Occurrence_ak_id in Claim_Occurrence table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Occurrence' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000' 
	
	UNION
	--- Claim Party Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Party_ak_id in Claim_Party table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Party' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Party where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000' 
	
	UNION
	-- Claim Party Occurrence Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Party_Occurrence_ak_id in Claim_Party_Occurrence table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Party_Occurrence' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Party_Occurrence where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000' 
	
	UNION
	-- Claim Party Relation Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Party_Occurrence_ak_id in Claim_Party_Relation table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Party_Relation' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Party_Relation where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000' 
	
	UNION
	-- Claim Representative Table 
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Rep_ak_id in Claim_Representative table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Representative' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Representative where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000' 
	
	UNION
	-- Claim Representative Occurrence 
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Rep_Occ_ak_id in Claim_Representative_Occurrence table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Representative_Occurrence' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Representative_Occurrence where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000' 
	
	UNION
	 -- Claimant Coverage Detail Table
	SELECT COUNT(*) as EDW_count, 'Count of Claimant_Cov_Det_ak_id in Claimant_Coverage_Detail table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 =  ' + CONVERT(varchar,count(*)) as check_out_message,'Claimant_Coverage_Detail' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Coverage_Detail where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Workers Comp Claimant Detail Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Party_Occurrence_ak_id in Workers_Comp_Claimant_Detail table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Workers_Comp_Claimant_Detail' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Workers_Comp_Claimant_Detail where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claim Transaction Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Trans_ak_id in Claim_Transaction table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Transaction' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Transaction where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claim Payment Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Pay_ak_id in Claim_Payment table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Payment' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Payment where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claimant Coverage Detail Reserve Calculation Table 
	SELECT COUNT(*) as EDW_count, 'Count of Claimant_Cov_Det_ak_id in Claimant_Coverage_Detail_Reserve_Calculation table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claimant_Coverage_Detail_Reserve_Calculation' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Coverage_Detail_Reserve_Calculation where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claimant Coverage Detail Calculation Table
	SELECT COUNT(*) as EDW_count, 'Count of Claimant_Cov_Det_ak_id in Claimant_Coverage_Detail_Calculation table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 =  ' + CONVERT(varchar,count(*)) as check_out_message,'Claimant_Coverage_Detail_Calculation' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Coverage_Detail_Calculation where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claimant Reserve Calculation Table 
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Party_Occurrence_ak_id in Claimant_Reserve_Calculation table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 =  ' + CONVERT(varchar,count(*)) as check_out_message,'Claimant_Reserve_Calculation' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Reserve_Calculation where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claimant Calculation Table 
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Party_Occurrence_ak_id in Claimant_Calculation table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claimant_Calculation' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Calculation where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claim Occurrence Reserve Calculation Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Occurrence_ak_id in Claim_Occurrence_Reserve_Calculation table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Occurrence_Reserve_Calculation' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence_Reserve_Calculation where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claim Occurrence Calculation Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Occurrence_ak_id in Claim_Occurrence_Calculation table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Occurrence_Calculation' as target_name from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence_Calculation where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claim Case Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Case_ak_id in Claim_Case table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' 
	+  CONVERT(varchar,count(*)) as check_out_message,'Claim_Case' as target_name 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Case where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claim Case Damage Detail Table 
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Case_Dam_det_ak_id in Claim_Case_Damage_Detail table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Case_Damage_Detail' as target_name 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Case_Damage_Detail where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claim Case Lien Detail Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Case_Lien_Det_ak_id in Claim_Case_Lien_Detail table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + 
	CONVERT(varchar,count(*)) as check_out_message,'Claim_Case_Lien_Detail' as target_name 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Case_Lien_Detail where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claim Reinsurance Transaction Table 
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Reins_trans_ak_id in Claim_Reinsurance_Transaction table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Reinsurance_Transaction' as target_name 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Reinsurance_Transaction where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claim Payment Category Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Pay_Ctgry_ak_id in Claim_Payment_Category table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + CONVERT(varchar,count(*)) as check_out_message,'Claim_Payment_Category' as target_name 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_payment_category where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
	
	UNION
	-- Claim Payment Occurrence Payment Table
	SELECT COUNT(*) as EDW_count, 'Count of Claim_Pay_Occurrence_pay_ak_id in Claim_Party_Occurrence_Payment table with crrnt_snpsht_flag = 1 that has Eff To Date not as 12/31/2100 = ' + 
	CONVERT(varchar,count(*)) as check_out_message,'Claim_Party_Occurrence_Payment' as target_name 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Party_Occurrence_Payment where crrnt_snpsht_flag = 1 and eff_to_date <> '2100-12-31 23:59:59.000'
),
EXP_default1 AS (
	SELECT
	EDW_count,
	check_out_message,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS wbmi_session_control_run_id,
	'InformS' AS created_user_id,
	SYSDATE AS created_date,
	'InformS' AS modified_user_id,
	SYSDATE AS modified_date,
	target_name,
	-- *INF*: 'E'
	-- 
	-- //E - Error, W - Warning
	'E' AS checkout_type_code
	FROM SQ_EDW_curr_row_AND_eff_from_date
),
FILTRANS1 AS (
	SELECT
	EDW_count, 
	check_out_message, 
	wbmi_session_control_run_id, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date, 
	target_name, 
	checkout_type_code
	FROM EXP_default1
	WHERE EDW_count>0
),
wbmi_checkout__EDW_curr_row_AND_eff_from_date AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID, 
	CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	EDW_count AS TARGET_COUNT, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM FILTRANS1
),
SQ_EDW_Calc_Highest_Eff_from_date AS (
	SELECT count(*), 'Count of Claimant_cov_det_ak_id from Claimant_Coverage_Detail_Reserve_Calculation table that does not have highest Eff_From_Date = '+ CONVERT(varchar,count(*)) as check_out_message,'Claimant_Coverage_Detail_Reserve_Calculation' as target_name
	FROM 
		(select claimant_cov_det_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Coverage_Detail_Reserve_Calculation                                                                                         
		group by claimant_cov_det_ak_id) A,
		(select claimant_cov_det_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Coverage_Detail_Reserve_Calculation
		where crrnt_snpsht_flag = 1
		group by claimant_cov_det_ak_id) B
	WHERE A.claimant_cov_det_ak_id = B.claimant_cov_det_ak_id
	and A.MAX_EFF_FROM <> B.MAX_EFF_FROM
	
	UNION
	
	SELECT count(*), 'Count of Claimant_cov_det_ak_id from Claimant_Coverage_Detail_Calculation table does not have highest Eff_From_Date = '+ CONVERT(varchar,count(*)) as check_out_message,'Claimant_Coverage_Detail_Calculation' as target_name
	FROM 
		(select claimant_cov_det_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Coverage_Detail_Calculation
		group by claimant_cov_det_ak_id) A,
		(select claimant_cov_det_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Coverage_Detail_Calculation
		where crrnt_snpsht_flag = 1
		group by claimant_cov_det_ak_id) B
	WHERE A.claimant_cov_det_ak_id = B.claimant_cov_det_ak_id
	and A.MAX_EFF_FROM <> B.MAX_EFF_FROM
	
	UNION
	
	SELECT count(*), 'Count of Claim_Party_Occurrence_ak_id from Claimant_Reserve_Calculation table does not have highest Eff_From_Date = '+ CONVERT(varchar,count(*)) as check_out_message,'Claimant_Reserve_Calculation' as target_name
	FROM 
		(select claim_party_occurrence_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Reserve_Calculation
		group by claim_party_occurrence_ak_id) A,
		(select claim_party_occurrence_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Reserve_Calculation
		where crrnt_snpsht_flag = 1
		group by claim_party_occurrence_ak_id) B
	WHERE A.claim_party_occurrence_ak_id = B.claim_party_occurrence_ak_id
	and A.MAX_EFF_FROM <> B.MAX_EFF_FROM
	
	UNION
	
	SELECT count(*), 'Count of Claim_Party_Occurrence_ak_id from Claimant_Calculation table does not have highest Eff_From_Date = '+ CONVERT(varchar,count(*)) as check_out_message,'Claimant_Calculation' as target_name 
	FROM  
		(select claim_party_occurrence_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Calculation
		group by claim_party_occurrence_ak_id) A,
		(select claim_party_occurrence_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claimant_Calculation
		where crrnt_snpsht_flag = 1
		group by claim_party_occurrence_ak_id) B
	WHERE A.claim_party_occurrence_ak_id = B.claim_party_occurrence_ak_id
	and A.MAX_EFF_FROM <> B.MAX_EFF_FROM
	
	UNION
	
	SELECT count(*), 'Count of Claim_Occurrence_ak_id from Claim_Occurrence_Reserve_Calculation table does not have highest Eff_From_Date = '+ CONVERT(varchar,count(*)) as check_out_message,'Claim_Occurrence_Reserve_Calculation' as target_name 
	FROM 
		(select claim_occurrence_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence_Reserve_Calculation
		group by claim_occurrence_ak_id) A,
		(select claim_occurrence_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence_Reserve_Calculation
		where crrnt_snpsht_flag = 1
		group by claim_occurrence_ak_id) B
	WHERE A.claim_occurrence_ak_id = B.claim_occurrence_ak_id
	and A.MAX_EFF_FROM <> B.MAX_EFF_FROM
	
	UNION
	
	SELECT count(*), 'Count of Claim_Occurrence_ak_id from Claim_Occurrence_Calculation table does not have highest Eff_From_Date = '+ CONVERT(varchar,count(*)) as check_out_message,'Claim_Occurrence_Calculation' as target_name
	FROM 
		(select claim_occurrence_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence_Calculation
		group by claim_occurrence_ak_id) A,
		(select claim_occurrence_ak_id, MAX(eff_from_date) as MAX_EFF_FROM
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.Claim_Occurrence_Calculation
		where crrnt_snpsht_flag = 1
		group by claim_occurrence_ak_id) B
	WHERE A.claim_occurrence_ak_id = B.claim_occurrence_ak_id
	and A.MAX_EFF_FROM <> B.MAX_EFF_FROM
),
EXP_default2 AS (
	SELECT
	EDW_count,
	check_out_message,
	@{pipeline().parameters.WBMI_SESSION_CONTROL_RUN_ID} AS wbmi_session_control_run_id,
	'InformS' AS created_user_id,
	SYSDATE AS created_date,
	'InformS' AS modified_user_id,
	SYSDATE AS modified_date,
	target_name,
	-- *INF*: 'E'
	-- 
	-- //E - Error, W - Warning
	'E' AS checkout_type_code
	FROM SQ_EDW_Calc_Highest_Eff_from_date
),
FILTRANS2 AS (
	SELECT
	EDW_count, 
	check_out_message, 
	wbmi_session_control_run_id, 
	created_user_id, 
	created_date, 
	modified_user_id, 
	modified_date, 
	target_name, 
	checkout_type_code
	FROM EXP_default2
	WHERE EDW_count>0
),
wbmi_checkout_EDW_Calc_Highest_Eff_from_date AS (
	INSERT INTO wbmi_checkout
	(wbmi_session_control_run_id, checkout_type_code, checkout_message, target_name, target_count, created_user_id, created_date, modified_user_id, modified_date)
	SELECT 
	WBMI_SESSION_CONTROL_RUN_ID, 
	CHECKOUT_TYPE_CODE, 
	check_out_message AS CHECKOUT_MESSAGE, 
	TARGET_NAME, 
	EDW_count AS TARGET_COUNT, 
	CREATED_USER_ID, 
	CREATED_DATE, 
	MODIFIED_USER_ID, 
	MODIFIED_DATE
	FROM FILTRANS2
),
SQ_wbmi_checkout AS (
	select 
	wbmi_checkout.checkout_message + ' <BR> <BR> ',
	wbmi_batch_control_run.email_address
	from 
	dbo.wbmi_checkout wbmi_checkout,
	dbo.wbmi_session_control_run wbmi_session_control_run,
	dbo.wbmi_batch_control_run wbmi_batch_control_run
	where
	wbmi_checkout.checkout_type_code = 'E' and 
	wbmi_checkout.wbmi_session_control_run_id = wbmi_session_control_run.wbmi_session_control_run_id and
	wbmi_session_control_run.current_ind = 'Y'  and 
	wbmi_session_control_run.wbmi_batch_control_run_id = wbmi_batch_control_run.wbmi_batch_control_run_id and
	rtrim(wbmi_batch_control_run.batch_name) = 'CLAIMS_SUPPORT_DATAWAREHOUSE'
	order by wbmi_checkout_id
),
EXP_Email_Subject AS (
	SELECT
	email_address,
	checkout_message,
	-- *INF*: 'There are errors in the Claims EDW data. Execution aborted (' || sysdate || ')'
	'There are errors in the Claims EDW data. Execution aborted (' || sysdate || ')' AS email_subject
	FROM SQ_wbmi_checkout
),
email_body AS (
	INSERT INTO email_body
	(FIELD1)
	SELECT 
	checkout_message AS FIELD1
	FROM EXP_Email_Subject
),
AGG_Distinct_Email_Id AS (
	SELECT
	email_address,
	email_subject
	FROM EXP_Email_Subject
	QUALIFY ROW_NUMBER() OVER (PARTITION BY email_address, email_subject ORDER BY NULL) = 1
),
email_subject AS (
	INSERT INTO email_subject
	(FIELD1)
	SELECT 
	email_subject AS FIELD1
	FROM AGG_Distinct_Email_Id
),
email_address AS (
	INSERT INTO email_address
	(FIELD1)
	SELECT 
	email_address AS FIELD1
	FROM AGG_Distinct_Email_Id
),
SQ_wbmi_checkout1 AS (
	select 
	wbmi_checkout.checkout_message + ' <BR> <BR> ',
	wbmi_batch_control_run.email_address
	from 
	dbo.wbmi_checkout wbmi_checkout,
	dbo.wbmi_session_control_run wbmi_session_control_run,
	dbo.wbmi_batch_control_run wbmi_batch_control_run
	where
	wbmi_checkout.checkout_type_code = 'E' and 
	wbmi_checkout.wbmi_session_control_run_id = wbmi_session_control_run.wbmi_session_control_run_id and
	wbmi_session_control_run.current_ind = 'Y'  and 
	wbmi_session_control_run.wbmi_batch_control_run_id = wbmi_batch_control_run.wbmi_batch_control_run_id and
	rtrim(wbmi_batch_control_run.batch_name)  = 'CLAIMS_SUPPORT_DATAWAREHOUSE'
	order by wbmi_checkout_id
),
EXP_Email_Subject1 AS (
	SELECT
	email_address,
	checkout_message,
	-- *INF*: Abort('There are issues with the EDW data')
	Abort('There are issues with the EDW data'
	) AS error
	FROM SQ_wbmi_checkout1
),
FIL_STOP_PROCESSING AS (
	SELECT
	checkout_message, 
	error
	FROM EXP_Email_Subject1
	WHERE FALSE
),
wbmi_checkout_dummy_target AS (
	INSERT INTO wbmi_checkout
	(checkout_message)
	SELECT 
	CHECKOUT_MESSAGE
	FROM FIL_STOP_PROCESSING
),