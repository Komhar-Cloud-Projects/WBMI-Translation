WITH
SQ_Balancing_of_Loss_Master_Calculation AS (
	DECLARE @month varchar(2)
	DECLARE @year varchar(4)
	DECLARE @yearmonth varchar(6)
	
	SELECT @month= MONTH(DATEADD(MONTH,-1,GETDATE())),@year= Year(GETDATE())
	
	SELECT  @yearmonth= @year + CASE LEN(@month) WHEN 1 THEN '0' + @month
	ELSE  @month END
	
	 ---------- Below Queries for balancing of Exceed Claims for Direct Business -----
	
	(SELECT 'STG' AS DESCR, 'Direct Business' as  Business_Type ,'EXCEED Claim' as Claim_type, 
	        LM_POLICY_SYMBOL + LM_POLICY_NUMBER + LM_MODULE_NUMBER                                          AS Policy_Key,
	        LM_DATE_OF_LOSS,
	        REPLICATE('0', 3 - Len(LM_LOSS_OCCURRENCE_NUMBER)) + Cast(LM_LOSS_OCCURRENCE_NUMBER AS VARCHAR) LM_LOSS_OCCURRENCE_NUMBER,
	        CASE
	          WHEN LEN(LM_TRANSACTION_CODE) = 0 THEN 'N/A'
	          ELSE LTRIM(RTRIM(LM_TRANSACTION_CODE))
	        END                            AS                                                                  LM_TRANSACTION_CODE,
	        LM_AMOUNT_OUTSTANDING,
	        LM_AMOUNT_PAID_LOSSES,
	        LM_AMOUNT_PAID_EXPENSES,
	        LM_EOM_UNPAID_LOSS_ADJ_EXP
	 FROM   @{pipeline().parameters.DB_NAME_STAGE}.dbo.loss_master_stage
	 WHERE  LM_ACCOUNT_ENTERED_DATE = @yearmonth
	        AND Len(LM_CLAIM_CONVERSION_NUMBER) <> 0
	        AND lm_kind_code = 'D'
	        
	 EXCEPT
	 SELECT 'STG' AS DESCR,'Direct Business' as  Business_Type ,'EXCEED Claim' as Claim_type,
	        pol_key                                   AS Policy_Key,
	        CONVERT(VARCHAR(8), claim_loss_date, 112) AS Date_of_Loss,
	        LTRIM(RTRIM(claim_occurrence_num))        AS Claim_Occurrence_num,
	        LTRIM(RTRIM(pms_trans_code))              AS pms_trans_code,
	        outstanding_amt,
	        paid_loss_amt,
	        paid_exp_amt,
	        eom_unpaid_loss_adjust_exp
	 FROM   dbo.loss_master_calculation LMC
	        INNER JOIN dbo.claim_occurrence CO WITH (nolock)
	          ON ( LMC.claim_occurrence_ak_id = CO.claim_occurrence_ak_id )
	 WHERE MONTH(Loss_Master_Run_Date) =@month AND
	       YEAR(Loss_Master_Run_Date)=@year 
	        AND CO.crrnt_snpsht_flag = 1
	        AND CO.s3p_claim_num <> 'N/A'
	        AND LMC.trans_kind_code = 'D')
	        
	        
	UNION
	
	(SELECT 'EDW' AS DESCR, 'Direct Business' as  Business_Type ,'EXCEED Claim' as Claim_type,
	        pol_key                                   AS Policy_Key,
	        CONVERT(VARCHAR(8), claim_loss_date, 112) AS Date_of_Loss,
	        LTRIM(RTRIM(claim_occurrence_num))        AS Claim_Occurrence_num,
	        LTRIM(RTRIM(pms_trans_code))              AS pms_trans_code,
	        outstanding_amt,
	        paid_loss_amt,
	        paid_exp_amt,
	        eom_unpaid_loss_adjust_exp
	 FROM   dbo.loss_master_calculation LMC
	        INNER JOIN dbo.claim_occurrence CO WITH (nolock)
	          ON ( LMC.claim_occurrence_ak_id = CO.claim_occurrence_ak_id )
	 WHERE  MONTH(Loss_Master_Run_Date) =@month AND
	        YEAR(Loss_Master_Run_Date)=@year
	        AND CO.crrnt_snpsht_flag = 1
	        AND CO.s3p_claim_num <> 'N/A'
	        AND LMC.trans_kind_code = 'D'
	 EXCEPT
	 SELECT 'EDW' AS DESCR, 'Direct Business' as  Business_Type ,'EXCEED Claim' as Claim_type,
	        LM_POLICY_SYMBOL + LM_POLICY_NUMBER + LM_MODULE_NUMBER                                          AS Policy_Key,
	        LM_DATE_OF_LOSS,
	        REPLICATE('0', 3 - Len(LM_LOSS_OCCURRENCE_NUMBER)) + Cast(LM_LOSS_OCCURRENCE_NUMBER AS VARCHAR) LM_LOSS_OCCURRENCE_NUMBER,
	        CASE
	          WHEN LEN(LM_TRANSACTION_CODE) = 0 THEN 'N/A'
	          ELSE LTRIM(RTRIM(LM_TRANSACTION_CODE))
	        END                          AS                                                                   LM_TRANSACTION_CODE,
	        LM_AMOUNT_OUTSTANDING,
	        LM_AMOUNT_PAID_LOSSES,
	        LM_AMOUNT_PAID_EXPENSES,
	        LM_EOM_UNPAID_LOSS_ADJ_EXP
	 FROM   @{pipeline().parameters.DB_NAME_STAGE}.dbo.loss_master_stage
	 WHERE  LM_ACCOUNT_ENTERED_DATE = @yearmonth
	        AND Len(LM_CLAIM_CONVERSION_NUMBER) <> 0
	        AND lm_kind_code = 'D') 
	        
	 ---------- Below Queries for balancing of Exceed Claims for Ceded Business -----
	UNION 
	        
	(SELECT 'STG' AS DESCR, 'Ceded Business' as  Business_Type ,'EXCEED Claim' as Claim_type, 
	        LM_POLICY_SYMBOL + LM_POLICY_NUMBER + LM_MODULE_NUMBER                                          AS Policy_Key,
	        LM_DATE_OF_LOSS,
	        REPLICATE('0', 3 - Len(LM_LOSS_OCCURRENCE_NUMBER)) + Cast(LM_LOSS_OCCURRENCE_NUMBER AS VARCHAR) LM_LOSS_OCCURRENCE_NUMBER,
	        CASE
	          WHEN LEN(LM_TRANSACTION_CODE) = 0 THEN 'N/A'
	          ELSE LTRIM(RTRIM(LM_TRANSACTION_CODE))
	        END                            AS                                                                  LM_TRANSACTION_CODE,
	        LM_AMOUNT_OUTSTANDING,
	        LM_AMOUNT_PAID_LOSSES,
	        LM_AMOUNT_PAID_EXPENSES,
	        LM_EOM_UNPAID_LOSS_ADJ_EXP
	 FROM   @{pipeline().parameters.DB_NAME_STAGE}.dbo.loss_master_stage
	 WHERE  LM_ACCOUNT_ENTERED_DATE = @yearmonth
	        AND Len(LM_CLAIM_CONVERSION_NUMBER) <> 0
	        AND lm_kind_code = 'C'
	        
	 EXCEPT
	 SELECT 'STG' AS DESCR,'Ceded Business' as  Business_Type ,'EXCEED Claim' as Claim_type, 
	        pol_key                                   AS Policy_Key,
	        CONVERT(VARCHAR(8), claim_loss_date, 112) AS Date_of_Loss,
	        LTRIM(RTRIM(claim_occurrence_num))        AS Claim_Occurrence_num,
	        LTRIM(RTRIM(pms_trans_code))              AS pms_trans_code,
	        outstanding_amt,
	        paid_loss_amt,
	        paid_exp_amt,
	        eom_unpaid_loss_adjust_exp
	 FROM   dbo.loss_master_calculation LMC
	        INNER JOIN dbo.claim_occurrence CO WITH (nolock)
	          ON ( LMC.claim_occurrence_ak_id = CO.claim_occurrence_ak_id )
	 WHERE MONTH(Loss_Master_Run_Date) =@month AND
	       YEAR(Loss_Master_Run_Date)=@year 
	        AND CO.crrnt_snpsht_flag = 1
	        AND CO.s3p_claim_num <> 'N/A'
	        AND LMC.trans_kind_code = 'C')
	        
	        
	UNION
	
	(SELECT 'EDW' AS DESCR, 'Ceded Business' as  Business_Type ,'EXCEED Claim' as Claim_type, 
	        pol_key                                   AS Policy_Key,
	        CONVERT(VARCHAR(8), claim_loss_date, 112) AS Date_of_Loss,
	        LTRIM(RTRIM(claim_occurrence_num))        AS Claim_Occurrence_num,
	        LTRIM(RTRIM(pms_trans_code))              AS pms_trans_code,
	        outstanding_amt,
	        paid_loss_amt,
	        paid_exp_amt,
	        eom_unpaid_loss_adjust_exp
	 FROM   dbo.loss_master_calculation LMC
	        INNER JOIN dbo.claim_occurrence CO WITH (nolock)
	          ON ( LMC.claim_occurrence_ak_id = CO.claim_occurrence_ak_id )
	 WHERE  MONTH(Loss_Master_Run_Date) =@month AND
	        YEAR(Loss_Master_Run_Date)=@year
	        AND CO.crrnt_snpsht_flag = 1
	        AND CO.s3p_claim_num <> 'N/A'
	        AND LMC.trans_kind_code = 'C'
	 EXCEPT
	 SELECT 'EDW' AS DESCR, 'Ceded Business' as  Business_Type ,'EXCEED Claim' as Claim_type, 
	        LM_POLICY_SYMBOL + LM_POLICY_NUMBER + LM_MODULE_NUMBER                                          AS Policy_Key,
	        LM_DATE_OF_LOSS,
	        REPLICATE('0', 3 - Len(LM_LOSS_OCCURRENCE_NUMBER)) + Cast(LM_LOSS_OCCURRENCE_NUMBER AS VARCHAR) LM_LOSS_OCCURRENCE_NUMBER,
	        CASE
	          WHEN LEN(LM_TRANSACTION_CODE) = 0 THEN 'N/A'
	          ELSE LTRIM(RTRIM(LM_TRANSACTION_CODE))
	        END                          AS                                                                   LM_TRANSACTION_CODE,
	        LM_AMOUNT_OUTSTANDING,
	        LM_AMOUNT_PAID_LOSSES,
	        LM_AMOUNT_PAID_EXPENSES,
	        LM_EOM_UNPAID_LOSS_ADJ_EXP
	 FROM   @{pipeline().parameters.DB_NAME_STAGE}.dbo.loss_master_stage
	 WHERE  LM_ACCOUNT_ENTERED_DATE = @yearmonth
	        AND Len(LM_CLAIM_CONVERSION_NUMBER) <> 0
	        AND lm_kind_code = 'C') 
	
	---------- Below Queries for balancing of PMS Claims for Direct Business -----
	UNION 
	
	(SELECT 'STG' AS DESCR, 'Direct Business' as  Business_Type ,'PMS Claim' as Claim_type,
	        LM_POLICY_SYMBOL + LM_POLICY_NUMBER + LM_MODULE_NUMBER                                          AS Policy_Key,
	        LM_DATE_OF_LOSS,
	        REPLICATE('0', 3 - Len(LM_LOSS_OCCURRENCE_NUMBER)) + Cast(LM_LOSS_OCCURRENCE_NUMBER AS VARCHAR) LM_LOSS_OCCURRENCE_NUMBER,
	        CASE
	          WHEN LEN(LM_TRANSACTION_CODE) = 0 THEN 'N/A'
	          ELSE LTRIM(RTRIM(LM_TRANSACTION_CODE))
	        END                            AS                                                                  LM_TRANSACTION_CODE,
	        LM_AMOUNT_OUTSTANDING,
	        LM_AMOUNT_PAID_LOSSES,
	        LM_AMOUNT_PAID_EXPENSES,
	        LM_EOM_UNPAID_LOSS_ADJ_EXP
	 FROM   @{pipeline().parameters.DB_NAME_STAGE}.dbo.loss_master_stage
	 WHERE  LM_ACCOUNT_ENTERED_DATE = @yearmonth
	        AND Len(LM_CLAIM_CONVERSION_NUMBER) = 0
	        AND lm_kind_code = 'D'
	        
	 EXCEPT
	 SELECT 'STG' AS DESCR,'Direct Business' as  Business_Type ,'PMS Claim' as Claim_type,
	        pol_key                                   AS Policy_Key,
	        CONVERT(VARCHAR(8), claim_loss_date, 112) AS Date_of_Loss,
	        LTRIM(RTRIM(claim_occurrence_num))        AS Claim_Occurrence_num,
	        LTRIM(RTRIM(pms_trans_code))              AS pms_trans_code,
	        outstanding_amt,
	        paid_loss_amt,
	        paid_exp_amt,
	        eom_unpaid_loss_adjust_exp
	 FROM   dbo.loss_master_calculation LMC
	        INNER JOIN dbo.claim_occurrence CO WITH (nolock)
	          ON ( LMC.claim_occurrence_ak_id = CO.claim_occurrence_ak_id )
	 WHERE MONTH(Loss_Master_Run_Date) =@month AND
	       YEAR(Loss_Master_Run_Date)=@year 
	        AND CO.crrnt_snpsht_flag = 1
	        AND CO.s3p_claim_num = 'N/A'
	        AND LMC.trans_kind_code = 'D')
	        
	        
	UNION
	
	(SELECT 'EDW' AS DESCR, 'Direct Business' as  Business_Type ,'PMS Claim' as Claim_type,
	        pol_key                                   AS Policy_Key,
	        CONVERT(VARCHAR(8), claim_loss_date, 112) AS Date_of_Loss,
	        LTRIM(RTRIM(claim_occurrence_num))        AS Claim_Occurrence_num,
	        LTRIM(RTRIM(pms_trans_code))              AS pms_trans_code,
	        outstanding_amt,
	        paid_loss_amt,
	        paid_exp_amt,
	        eom_unpaid_loss_adjust_exp
	 FROM   dbo.loss_master_calculation LMC
	        INNER JOIN dbo.claim_occurrence CO WITH (nolock)
	          ON ( LMC.claim_occurrence_ak_id = CO.claim_occurrence_ak_id )
	 WHERE  MONTH(Loss_Master_Run_Date) =@month AND
	        YEAR(Loss_Master_Run_Date)=@year
	        AND CO.crrnt_snpsht_flag = 1
	        AND CO.s3p_claim_num = 'N/A'
	        AND LMC.trans_kind_code = 'D'
	 EXCEPT
	  SELECT 'EDW' AS DESCR, 'Direct Business' as  Business_Type ,'PMS Claim' as Claim_type,
	        LM_POLICY_SYMBOL + LM_POLICY_NUMBER + LM_MODULE_NUMBER                                          AS Policy_Key,
	        LM_DATE_OF_LOSS,
	        REPLICATE('0', 3 - Len(LM_LOSS_OCCURRENCE_NUMBER)) + Cast(LM_LOSS_OCCURRENCE_NUMBER AS VARCHAR) LM_LOSS_OCCURRENCE_NUMBER,
	        CASE
	          WHEN LEN(LM_TRANSACTION_CODE) = 0 THEN 'N/A'
	          ELSE LTRIM(RTRIM(LM_TRANSACTION_CODE))
	        END                          AS                                                                   LM_TRANSACTION_CODE,
	        LM_AMOUNT_OUTSTANDING,
	        LM_AMOUNT_PAID_LOSSES,
	        LM_AMOUNT_PAID_EXPENSES,
	        LM_EOM_UNPAID_LOSS_ADJ_EXP
	 FROM   @{pipeline().parameters.DB_NAME_STAGE}.dbo.loss_master_stage
	 WHERE  LM_ACCOUNT_ENTERED_DATE = @yearmonth
	        AND Len(LM_CLAIM_CONVERSION_NUMBER) = 0
	        AND lm_kind_code = 'D')
	        
	 ---------- Below Queries for balancing of PMS Claims for Ceded Business -----
	UNION 
	
	(SELECT 'STG' AS DESCR, 'Ceded Business' as  Business_Type ,'PMS Claim' as Claim_type,
	        LM_POLICY_SYMBOL + LM_POLICY_NUMBER + LM_MODULE_NUMBER                                          AS Policy_Key,
	        LM_DATE_OF_LOSS,
	        REPLICATE('0', 3 - Len(LM_LOSS_OCCURRENCE_NUMBER)) + Cast(LM_LOSS_OCCURRENCE_NUMBER AS VARCHAR) LM_LOSS_OCCURRENCE_NUMBER,
	        CASE
	          WHEN LEN(LM_TRANSACTION_CODE) = 0 THEN 'N/A'
	          ELSE LTRIM(RTRIM(LM_TRANSACTION_CODE))
	        END                            AS                                                                  LM_TRANSACTION_CODE,
	        LM_AMOUNT_OUTSTANDING,
	        LM_AMOUNT_PAID_LOSSES,
	        LM_AMOUNT_PAID_EXPENSES,
	        LM_EOM_UNPAID_LOSS_ADJ_EXP
	 FROM   @{pipeline().parameters.DB_NAME_STAGE}.dbo.loss_master_stage
	 WHERE  LM_ACCOUNT_ENTERED_DATE = @yearmonth
	        AND Len(LM_CLAIM_CONVERSION_NUMBER) = 0
	        AND lm_kind_code = 'C'
	 EXCEPT
	 SELECT 'STG' AS DESCR,'Ceded Business' as  Business_Type ,'PMS Claim' as Claim_type,
	        pol_key                                   AS Policy_Key,
	        CONVERT(VARCHAR(8), claim_loss_date, 112) AS Date_of_Loss,
	        LTRIM(RTRIM(claim_occurrence_num))        AS Claim_Occurrence_num,
	        LTRIM(RTRIM(pms_trans_code))              AS pms_trans_code,
	        outstanding_amt,
	        paid_loss_amt,
	        paid_exp_amt,
	        eom_unpaid_loss_adjust_exp
	 FROM   dbo.loss_master_calculation LMC
	        INNER JOIN dbo.claim_occurrence CO WITH (nolock)
	          ON ( LMC.claim_occurrence_ak_id = CO.claim_occurrence_ak_id )
	 WHERE MONTH(Loss_Master_Run_Date) =@month AND
	       YEAR(Loss_Master_Run_Date)=@year 
	        AND CO.crrnt_snpsht_flag = 1
	        AND CO.s3p_claim_num = 'N/A'
	        AND LMC.trans_kind_code = 'C')
	        
	UNION
	
	(SELECT 'EDW' AS DESCR, 'Ceded Business' as  Business_Type ,'PMS Claim' as Claim_type,
	        pol_key                                   AS Policy_Key,
	        CONVERT(VARCHAR(8), claim_loss_date, 112) AS Date_of_Loss,
	        LTRIM(RTRIM(claim_occurrence_num))        AS Claim_Occurrence_num,
	        LTRIM(RTRIM(pms_trans_code))              AS pms_trans_code,
	        outstanding_amt,
	        paid_loss_amt,
	        paid_exp_amt,
	        eom_unpaid_loss_adjust_exp
	 FROM   dbo.loss_master_calculation LMC
	        INNER JOIN dbo.claim_occurrence CO WITH (nolock)
	          ON ( LMC.claim_occurrence_ak_id = CO.claim_occurrence_ak_id )
	 WHERE  MONTH(Loss_Master_Run_Date) =@month AND
	        YEAR(Loss_Master_Run_Date)=@year
	        AND CO.crrnt_snpsht_flag = 1
	        AND CO.s3p_claim_num = 'N/A'
	        AND LMC.trans_kind_code = 'C'
	 EXCEPT
	 SELECT 'EDW' AS DESCR, 'Ceded Business' as  Business_Type ,'PMS Claim' as Claim_type,
	        LM_POLICY_SYMBOL + LM_POLICY_NUMBER + LM_MODULE_NUMBER                                          AS Policy_Key,
	        LM_DATE_OF_LOSS,
	        REPLICATE('0', 3 - Len(LM_LOSS_OCCURRENCE_NUMBER)) + Cast(LM_LOSS_OCCURRENCE_NUMBER AS VARCHAR) LM_LOSS_OCCURRENCE_NUMBER,
	        CASE
	          WHEN LEN(LM_TRANSACTION_CODE) = 0 THEN 'N/A'
	          ELSE LTRIM(RTRIM(LM_TRANSACTION_CODE))
	        END                          AS                                                                   LM_TRANSACTION_CODE,
	        LM_AMOUNT_OUTSTANDING,
	        LM_AMOUNT_PAID_LOSSES,
	        LM_AMOUNT_PAID_EXPENSES,
	        LM_EOM_UNPAID_LOSS_ADJ_EXP
	 FROM   @{pipeline().parameters.DB_NAME_STAGE}.dbo.loss_master_stage
	 WHERE  LM_ACCOUNT_ENTERED_DATE = @yearmonth
	        AND Len(LM_CLAIM_CONVERSION_NUMBER) = 0
	        AND lm_kind_code = 'C')
	        
	ORDER BY 2,3,4,5
),
EXP_Default AS (
	SELECT
	lm_reinsurance_cession_number,
	lm_draft_number,
	lm_accident_description,
	lm_policy_symbol,
	lm_date_of_loss,
	lm_account_entered_date,
	lm_transaction_code,
	lm_amount_outstanding,
	lm_amount_paid_losses,
	lm_amount_paid_expenses,
	lm_eom_unpaid_loss_adj_exp
	FROM SQ_Balancing_of_Loss_Master_Calculation
),
Lossmaster_Balancing_Report AS (
	INSERT INTO Lossmaster_Balancing_Report
	(SOURCE_TYPE, BUSINESS_TYPE, CLAIM_TYPE, POLICY_KEY, DATE_OF_LOSS, LOSS_OCCURRENCE_NUM, AMOUNT_OUTSTANDING, AMOUNT_PAID_LOSSES, AMOUNT_PAID_EXPENSES, EOM_UNPAID_LOSS_ADJ_EXP, TRANSACTION_CODE)
	SELECT 
	lm_reinsurance_cession_number AS SOURCE_TYPE, 
	lm_draft_number AS BUSINESS_TYPE, 
	lm_accident_description AS CLAIM_TYPE, 
	lm_policy_symbol AS POLICY_KEY, 
	lm_date_of_loss AS DATE_OF_LOSS, 
	lm_account_entered_date AS LOSS_OCCURRENCE_NUM, 
	lm_transaction_code AS AMOUNT_OUTSTANDING, 
	lm_amount_outstanding AS AMOUNT_PAID_LOSSES, 
	lm_amount_paid_losses AS AMOUNT_PAID_EXPENSES, 
	lm_amount_paid_expenses AS EOM_UNPAID_LOSS_ADJ_EXP, 
	lm_eom_unpaid_loss_adj_exp AS TRANSACTION_CODE
	FROM EXP_Default
),