WITH
SQ_policy AS (
	SELECT policy.pol_id,
	case when convert(varchar(6),policy.eff_from_date,112)='180001' then policy.created_date else policy.eff_from_date end compare_date,
	       policy.pol_sym,
	       policy.pol_num,
	       policy.pol_mod,
	       policy.pol_eff_date,
	       policy.pol_exp_date,
	       policy.pol_cancellation_date,
	       policy.pol_status_code,
	       policy.renl_code
	FROM
	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy  policy
	WHERE 
	policy.crrnt_snpsht_flag =1
	and (policy.pol_status_code <> 'N'
	or (policy.pol_status_code = 'N'
	                   AND policy.pol_exp_date >= dateadd(yyyy, -1, getdate())))
	--Fix for Defect EDWP-3378 to add filter to select only PMS records 
	 AND policy.source_sys_id='PMS'
	--the last part of the filter condition checks for only those policies that are either "Not InForce(Non-Expired)" or policies that have been expired in the last year. Policies that have been expired more than a year ago, we will not calculate the status of those ones again'
),
EXP_values AS (
	SELECT
	pol_id,
	pol_sym,
	pol_num,
	pol_mod,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	pol_status_code,
	renl_code,
	-- *INF*: ltrim(rtrim(pol_sym)) || ltrim(rtrim(pol_num)) || ltrim(rtrim(pol_mod))
	ltrim(rtrim(pol_sym
		)
	) || ltrim(rtrim(pol_num
		)
	) || ltrim(rtrim(pol_mod
		)
	) AS v_pol_key,
	v_pol_key AS out_pol_key,
	compare_date
	FROM SQ_policy
),
LKP_4514_Stage_pol_cancellation_date AS (
	SELECT
	pol_cancellationdate,
	sar_reason_amend_code,
	sar_entrd_date,
	sar_transaction,
	in_policy_key,
	pol_key
	FROM (
		SELECT PIF4514.pol_cancellationdate AS pol_cancellationdate,
		PIF4514.sar_reason_amend_code AS sar_reason_amend_code,
		PIF4514.sar_entrd_date AS sar_entrd_date,
		PIF4514.sar_transaction AS sar_transaction,
		PIF4514.pol_key AS pol_key FROM
		(
		SELECT      
			  case  when sar_transaction in (20,21) then min(
					                        concat(
											cast(ltrim(rtrim(a.sar_trans_eff_year)) as varchar(4)),
											ISNULL(RIGHT('00'+CONVERT(VARCHAR(3),a.sar_trans_eff_month),2),'00'), 
											ISNULL(RIGHT('00'+CONVERT(VARCHAR(3),a.sar_trans_eff_day),2),'00')
					       					))
						
				    when  sar_transaction in (23,25,29) then max(
					    				concat(
										cast(ltrim(rtrim(a.sar_trans_eff_year)) as varchar(4)),
										ISNULL(RIGHT('00'+CONVERT(VARCHAR(3),a.sar_trans_eff_month),2),'00'), 
										ISNULL(RIGHT('00'+CONVERT(VARCHAR(3),a.sar_trans_eff_day),2),'00')
										))	 
						 
		            else '21001231' end  as pol_cancellationdate,
		            a.sar_rsn_amend_one + a.sar_rsn_amend_two + a.sar_rsn_amend_three as sar_reason_amend_code,
					max(a.sar_entrd_date) as sar_entrd_date,
					      SAR_TRANSACTION as sar_transaction,
						  ltrim(rtrim(pif_symbol)) + ltrim(rtrim(pif_policy_number)) + ltrim(rtrim(pif_module)) as pol_key
		        from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME}  a ,
					   (SELECT max(cc.sar_entrd_date)      AS sar_entrd_date,
							  cc.pif_symbol +cc.pif_policy_number+cc.pif_module as policy_key
						FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME} cc
						WHERE cc.sar_rsn_amend_one IN ('S', 'P') and sar_transaction in (20,21,23,25,29)
						GROUP BY cc.pif_symbol +cc.pif_policy_number+cc.pif_module
						)  b
		        where a.sar_rsn_amend_one IN ('S', 'P')
				    	and a.pif_symbol +a.pif_policy_number+a.pif_module = b.policy_key
						and a.sar_entrd_date = b.sar_entrd_date 
						and a.sar_transaction in(20,21,23,25,29)
		--and b.policy_key like 'CPD170650900'
					 GROUP BY 		
					 ltrim(rtrim(pif_symbol)) + ltrim(rtrim(pif_policy_number)) + ltrim(rtrim(pif_module))
					,a.sar_rsn_amend_one + a.sar_rsn_amend_two + a.sar_rsn_amend_three
					,SAR_TRANSACTION
					) PIF4514
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_cancellationdate DESC) = 1
),
EXP_Detect_CancellationDate_And_Status AS (
	SELECT
	EXP_values.pol_id,
	EXP_values.pol_eff_date,
	EXP_values.pol_exp_date,
	EXP_values.pol_cancellation_date,
	EXP_values.pol_status_code AS pol_cancellation_ind,
	EXP_values.pol_status_code,
	EXP_values.renl_code,
	LKP_4514_Stage_pol_cancellation_date.pol_cancellationdate,
	LKP_4514_Stage_pol_cancellation_date.sar_reason_amend_code,
	LKP_4514_Stage_pol_cancellation_date.sar_entrd_date,
	LKP_4514_Stage_pol_cancellation_date.sar_transaction,
	EXP_values.compare_date,
	-- *INF*: IIF(IS_DATE(pol_cancellationdate,'yyyymmdd')
	-- ,TO_DATE(pol_cancellationdate,'yyyymmdd')
	-- ,TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'))
	-- 
	-- --we are building the entire trans_eff_date with the above expression. if it is not a correct date, then populate a default date of 12/31/2100
	IFF(IS_DATE(pol_cancellationdate, 'yyyymmdd'
		),
		TO_DATE(pol_cancellationdate, 'yyyymmdd'
		),
		TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
		)
	) AS v_sar_trans_eff_date,
	-- *INF*: IIF( NOT ISNULL(v_sar_trans_eff_date) AND renl_code = '9',    v_sar_trans_eff_date,
	-- TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	-- )
	IFF(v_sar_trans_eff_date IS NULL 
		AND renl_code =NOT  '9',
		v_sar_trans_eff_date,
		TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
		)
	) AS v_new_cancellation_date,
	v_new_cancellation_date AS out_new_cancellation_date,
	-- *INF*: iif( pol_eff_date<=compare_date AND 
	-- 	      compare_date< (iif (pol_exp_date< v_new_cancellation_date, pol_exp_date,v_new_cancellation_date)),'I' ,
	-- iif(v_new_cancellation_date<=compare_date OR (v_new_cancellation_date=pol_eff_date AND compare_date<=pol_eff_date),'C',
	-- iif(compare_date>=pol_exp_date,'N' ,
	-- iif(compare_date<pol_eff_date AND (v_new_cancellation_date>compare_date OR v_new_cancellation_date>pol_eff_date),'F', 'N/A' 
	-- )))
	-- )
	IFF(pol_eff_date <= compare_date 
		AND compare_date < ( IFF(pol_exp_date < v_new_cancellation_date,
				pol_exp_date,
				v_new_cancellation_date
			) 
		),
		'I',
		IFF(v_new_cancellation_date <= compare_date 
			OR ( v_new_cancellation_date = pol_eff_date 
				AND compare_date <= pol_eff_date 
			),
			'C',
			IFF(compare_date >= pol_exp_date,
				'N',
				IFF(compare_date < pol_eff_date 
					AND ( v_new_cancellation_date > compare_date 
						OR v_new_cancellation_date > pol_eff_date 
					),
					'F',
					'N/A'
				)
			)
		)
	) AS v_new_pol_status_code,
	v_new_pol_status_code AS out_new_pol_status_code,
	-- *INF*: IIF(NOT ISNULL(sar_reason_amend_code) AND renl_code='9',sar_reason_amend_code,'N/A')
	IFF(sar_reason_amend_code IS NULL 
		AND renl_code =NOT  '9',
		sar_reason_amend_code,
		'N/A'
	) AS v_PolicyCancellationReasonCode,
	v_PolicyCancellationReasonCode AS out_PolicyCancellationReasonCode,
	-- *INF*: DECODE(true,
	-- pol_cancellation_date != v_new_cancellation_date,'Y'
	-- ,pol_status_code != v_new_pol_status_code,'Y'
	-- ,'N')
	-- 
	-- --set the status to  'Y' if either cancellation date or policy satus codes are different else set it to N. 
	-- 
	DECODE(true,
		pol_cancellation_date != v_new_cancellation_date, 'Y',
		pol_status_code != v_new_pol_status_code, 'Y',
		'N'
	) AS Update_Flag,
	sysdate AS Modified_date
	FROM EXP_values
	LEFT JOIN LKP_4514_Stage_pol_cancellation_date
	ON LKP_4514_Stage_pol_cancellation_date.pol_key = EXP_values.out_pol_key
),
FIL_Update AS (
	SELECT
	pol_id, 
	out_new_cancellation_date AS new_pol_cancellation_date, 
	out_new_pol_status_code AS new_pol_status_code, 
	Update_Flag, 
	Modified_date, 
	out_PolicyCancellationReasonCode AS PolicyCancellationReasonCode
	FROM EXP_Detect_CancellationDate_And_Status
	WHERE Update_Flag = 'Y'

--only pass the records if either cancellation date or policy satus codes are different
),
LKP_sup_policy_status_code AS (
	SELECT
	sup_pol_status_code_id,
	pol_status_code
	FROM (
		SELECT 
			sup_pol_status_code_id,
			pol_status_code
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.sup_policy_status_code
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_status_code ORDER BY sup_pol_status_code_id) = 1
),
Upd_Target AS (
	SELECT
	FIL_Update.pol_id, 
	FIL_Update.new_pol_cancellation_date, 
	FIL_Update.new_pol_status_code, 
	FIL_Update.Update_Flag, 
	FIL_Update.Modified_date, 
	LKP_sup_policy_status_code.sup_pol_status_code_id, 
	FIL_Update.PolicyCancellationReasonCode
	FROM FIL_Update
	LEFT JOIN LKP_sup_policy_status_code
	ON LKP_sup_policy_status_code.pol_status_code = FIL_Update.new_pol_status_code
),
TGT_policy_Update AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy AS T
	USING Upd_Target AS S
	ON T.pol_id = S.pol_id
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.modified_date = S.Modified_date, T.pol_cancellation_date = S.new_pol_cancellation_date, T.pol_cancellation_rsn_code = S.PolicyCancellationReasonCode, T.pol_status_code = S.new_pol_status_code, T.sup_pol_status_code_id = S.sup_pol_status_code_id
),