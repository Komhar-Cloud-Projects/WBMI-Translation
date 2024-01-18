WITH
SQ_WCAggravatedInequityExtract AS (
	SELECT WCAggravatedInequityExtract.PolicyKey, WCAggravatedInequityExtract.PolicyEffectiveDate, 
	WCAggravatedInequityExtract.ClaimNumber, WCAggravatedInequityExtract.PaidIndemnityAmount, 
	WCAggravatedInequityExtract.PaidMedicalAmount, WCAggravatedInequityExtract.ClaimantNumber,
	 WCAggravatedInequityExtract.ClaimantFullName, WCAggravatedInequityExtract.ClaimLossDate,
	 WCAggravatedInequityExtract.ClaimantCloseDate, WCAggravatedInequityExtract.Qualify, 
	 WCAggravatedInequityExtract.Comments, WCAggravatedInequityExtract.MonthsSinceUnitStat, 
	 WCAggravatedInequityExtract.CustomerNumber, WCAggravatedInequityExtract.RatingState 
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.WCAggravatedInequityExtract
	where WCAggravatedInequityExtract.AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE}
	ORDER BY  WCAggravatedInequityExtract.PolicyEffectiveDate
),
EXP_Filename AS (
	SELECT
	ClaimantFullName,
	ClaimantNumber,
	PolicyKey,
	PolicyEffectiveDate,
	ClaimNumber,
	ClaimLossDate,
	ClaimantCloseDate,
	PaidIndemnityAmount,
	PaidMedicalAmount,
	CustomerNumber,
	Qualify,
	Comments,
	MonthsSinceUnitStat,
	-- *INF*: 'WC_Aggravated_Inequity_Data_File_'||RatingState||'_'||TO_CHAR(ClaimantCloseDate,'YYYYMM')||'.CSV'
	-- --'WC_Aggravated_Inequity_Data_File_'||RatingState||'_'||TO_CHAR(ADD_TO_DATE(SYSDATE ,'MONTH',-1),'YYYYMM')||'.CSV'
	'WC_Aggravated_Inequity_Data_File_' || RatingState || '_' || TO_CHAR(ClaimantCloseDate, 'YYYYMM') || '.CSV' AS v_output_file_name,
	v_output_file_name AS Output_file_name,
	RatingState
	FROM SQ_WCAggravatedInequityExtract
),
RTR_ByState AS (
	SELECT
	ClaimantFullName,
	ClaimantNumber,
	PolicyKey,
	PolicyEffectiveDate,
	ClaimNumber,
	ClaimLossDate,
	ClaimantCloseDate,
	PaidIndemnityAmount,
	PaidMedicalAmount,
	CustomerNumber,
	Qualify,
	Comments,
	Output_file_name,
	RatingState
	FROM EXP_Filename
),
RTR_ByState_WI AS (SELECT * FROM RTR_ByState WHERE RatingState='WI'),
RTR_ByState_MN AS (SELECT * FROM RTR_ByState WHERE RatingState='MN'),
RTR_ByState_MI AS (SELECT * FROM RTR_ByState WHERE RatingState='MI'),
WC_Aggrevated_Inequity_MI AS (
	INSERT INTO WC_Aggrevated_Inequity
	(FileName1, Policy_key, Date_Of_Loss, Claim_Number, Claimant_Number, Claimant_Full_name, Policy_Effective_Date, Claimant_Close_Date, Paid_Indemnity, Paid_Medical, CustomerNumber, Qualify, Comments, RatingState)
	SELECT 
	Output_file_name AS FILENAME1, 
	PolicyKey AS POLICY_KEY, 
	ClaimLossDate AS DATE_OF_LOSS, 
	ClaimNumber AS CLAIM_NUMBER, 
	ClaimantNumber AS CLAIMANT_NUMBER, 
	ClaimantFullName AS CLAIMANT_FULL_NAME, 
	PolicyEffectiveDate AS POLICY_EFFECTIVE_DATE, 
	ClaimantCloseDate AS CLAIMANT_CLOSE_DATE, 
	PaidIndemnityAmount AS PAID_INDEMNITY, 
	PaidMedicalAmount AS PAID_MEDICAL, 
	CUSTOMERNUMBER, 
	QUALIFY, 
	COMMENTS, 
	RATINGSTATE
	FROM RTR_ByState_MI
),
WC_Aggrevated_Inequity_WI AS (
	INSERT INTO WC_Aggrevated_Inequity
	(FileName1, Policy_key, Date_Of_Loss, Claim_Number, Claimant_Number, Claimant_Full_name, Policy_Effective_Date, Claimant_Close_Date, Paid_Indemnity, Paid_Medical, CustomerNumber, Qualify, Comments, RatingState)
	SELECT 
	Output_file_name AS FILENAME1, 
	PolicyKey AS POLICY_KEY, 
	ClaimLossDate AS DATE_OF_LOSS, 
	ClaimNumber AS CLAIM_NUMBER, 
	ClaimantNumber AS CLAIMANT_NUMBER, 
	ClaimantFullName AS CLAIMANT_FULL_NAME, 
	PolicyEffectiveDate AS POLICY_EFFECTIVE_DATE, 
	ClaimantCloseDate AS CLAIMANT_CLOSE_DATE, 
	PaidIndemnityAmount AS PAID_INDEMNITY, 
	PaidMedicalAmount AS PAID_MEDICAL, 
	CUSTOMERNUMBER, 
	QUALIFY, 
	COMMENTS, 
	RATINGSTATE
	FROM RTR_ByState_WI
),
WC_Aggrevated_Inequity_MN AS (
	INSERT INTO WC_Aggrevated_Inequity
	(FileName1, Policy_key, Date_Of_Loss, Claim_Number, Claimant_Number, Claimant_Full_name, Policy_Effective_Date, Claimant_Close_Date, Paid_Indemnity, Paid_Medical, CustomerNumber, Qualify, Comments, RatingState)
	SELECT 
	Output_file_name AS FILENAME1, 
	PolicyKey AS POLICY_KEY, 
	ClaimLossDate AS DATE_OF_LOSS, 
	ClaimNumber AS CLAIM_NUMBER, 
	ClaimantNumber AS CLAIMANT_NUMBER, 
	ClaimantFullName AS CLAIMANT_FULL_NAME, 
	PolicyEffectiveDate AS POLICY_EFFECTIVE_DATE, 
	ClaimantCloseDate AS CLAIMANT_CLOSE_DATE, 
	PaidIndemnityAmount AS PAID_INDEMNITY, 
	PaidMedicalAmount AS PAID_MEDICAL, 
	CUSTOMERNUMBER, 
	QUALIFY, 
	COMMENTS, 
	RATINGSTATE
	FROM RTR_ByState_MN
),