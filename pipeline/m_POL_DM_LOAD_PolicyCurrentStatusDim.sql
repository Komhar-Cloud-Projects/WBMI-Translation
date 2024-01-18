WITH
LKP_ReasonAmendedCode_DCT AS (
	SELECT
	ReasonAmendedCodeDimId,
	pol_key,
	pol_cancellation_date
	FROM (
		SELECT RAC.ReasonAmendedCodeDimId AS ReasonAmendedCodeDimId,
		RAC.pol_key AS pol_key,
		RAC.pol_cancellation_date AS pol_cancellation_date FROM 
		(
		select  
		distinct d.ReasonAmendedCodeDimId  as ReasonAmendedCodeDimId ,
		PolicyNumber+ISNULL(RIGHT('00'+CONVERT(VARCHAR(3), PolicyVersion),2),'00') as pol_key,
		case when CancellationDate is null then convert(varchar(8),TransactionEffectiveDate,112) 
		     when cancellationdate>TransactionEffectiveDate then convert(varchar(8),TransactionEffectiveDate,112) 
			 else convert(varchar(8),cancellationdate,112)   end pol_cancellation_date	 
		from @{pipeline().parameters.STAGE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.STAGE_TABLE_NAME}
		 join @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.sup_reason_amended_code r on reasoncode=r.rsn_amended_code
		  join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ReasonAmendedCodeDim d on r.StandardReasonAmendedCode=d.ReasonAmendedCode 
		where PolicyStatus<>'Quote' and TransactionState='committed' and policystatus='Cancelled' 
		and r.source_sys_id='DCT') RAC
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key,pol_cancellation_date ORDER BY ReasonAmendedCodeDimId DESC) = 1
),
SQ_V2Policy_LatestStatusPerMonth AS (
	Declare @previousmonth varchar(6)
	Set @previousmonth= convert(varchar(6),dateadd(mm,@{pipeline().parameters.NO_OF_MONTHS},getdate()),112)
	
	SELECT 
	POL.eff_from_date,
	POL.eff_to_date,
	POL.source_sys_id,
	POL.created_date,
	POL.pol_ak_id,
	POL.POL_KEY,
	POL.pol_eff_date,
	POL.pol_exp_date,
	POL.pol_cancellation_date,
	POL.Pol_compare_date,
	POL.CalendarEndOfMonthDate,
	POL.Rundate,
	POL.pol_cancellation_rsn_code,
	POL.pol_status_code  
	FROM
	(
				  SELECT  V2POLICY.pol_key,
						  V2POLICY.Pol_compare_date ,
	                      V2POLICY.created_date,
	                      V2POLICY.eff_from_date,
						  V2POLICY.eff_to_date,
	                                  V2POLICY.source_sys_id,
						  V2POLICY.pol_eff_date,
						  V2POLICY.pol_exp_date,
						  V2POLICY.pol_cancellation_date,
						  V2POLICY.pol_status_code,
						  V2POLICY.pol_cancellation_rsn_code,
						  V2POLICY.pol_ak_id,
						  CAL.CalendarEndOfMonthDate,
	                     dateadd(ss,-1,dateadd(dd,1,CAL.CalendarEndOfMonthDate))  Rundate,
						 Rank() over(partition by V2POLICY.pol_key,convert(varchar(6),V2POLICY.Pol_compare_date,112) order by V2POLICY.Pol_compare_date desc,V2POLICY.eff_from_date ) Rank_record
							---Rank record is to rank the records per policy key and per month
								FROM (SELECT POL1.POL_KEY,
								 POL1.Pol_compare_date,
								 POL1.created_date,
								  POL1.eff_from_date,
									POL1.eff_to_date,
								  POL1.source_sys_id,
		 						  POL1.pol_eff_date,
								  POL1.pol_exp_date,
								  POL1.pol_cancellation_date,
								  POL1.pol_status_code,
		                          POL1.pol_cancellation_rsn_code,
								  POL1.pol_ak_id
								  
					       from (SELECT POL.POL_KEY,
												case when convert(varchar(6),POL.eff_from_date,112)='180001' then dateadd(dd,-1,POL.Created_date) else POL.eff_from_date end Pol_compare_date,
																			 POL.created_date,
																			  POL.eff_from_date,											
																			  POL.eff_to_date,																  
																			  POL.source_sys_id,
																			  POL.pol_eff_date,
																			  POL.pol_exp_date,
																			  POL.pol_cancellation_date,
																			  POL.pol_status_code,
																			  POL.pol_cancellation_rsn_code,
																			  POL.pol_ak_id,
	Rank() over(partition by POL.pol_key,convert(varchar(6),case when convert(varchar(6),POL.eff_from_date,112)='180001' then dateadd(dd,-1,POL.Created_date) else POL.eff_from_date end,112) order by  POL.eff_from_date   desc ) Rank_per_Month
	---Rank record is to rank the records per policy key and per month
																	   from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.POLICY POL
																			   WHERE  POL.source_sys_id='DCT'   
											@{pipeline().parameters.WHERE_CLAUSE_DCT}
											) POL1 WHERE POL1.Rank_per_Month=1
											 
								UNION
	--for PMS
									SELECT POL.POL_KEY,
								 case when convert(varchar(6),POL.eff_from_date,112)='180001' then POL.Created_date else POL.eff_from_date end Pol_compare_date,
								 POL.created_date,
								  POL.eff_from_date,
								  POL.eff_to_date,
								  POL.source_sys_id,
		 						  POL.pol_eff_date,
								  POL.pol_exp_date,
								  POL.pol_cancellation_date,
								  POL.pol_status_code,
		                          POL.pol_cancellation_rsn_code,
								  POL.pol_ak_id
					       from @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.POLICY POL where POL.source_sys_id='PMS'  
						   @{pipeline().parameters.WHERE_CLAUSE_PMS}				    
	                 	                	      ) V2POLICY 
					INNER JOIN  (SELECT DISTINCT CalendarStartOfMonthDate,CalendarEndOfMonthDate  FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.calendar_dim) CAL
					on 
					(
					(dateadd(ss,-1,dateadd(dd,1,CAL.CalendarEndOfMonthDate)) between V2POLICY.Pol_compare_date and V2POLICY.eff_to_date
					and convert(varchar(6),V2POLICY.pol_exp_date,112)>= convert(varchar(6),CAL.CalendarEndOfMonthDate,112))
					or   --- this condition is to pul the policies came to EDW after expiration
					(convert(varchar(6),CAL.CalendarEndOfMonthDate,112)>convert(varchar(6),V2POLICY.pol_exp_date,112)
					and V2POLICY.Pol_compare_date 
				    between CAL.CalendarStartOfMonthDate and dateadd(ss,-1,dateadd(dd,1,CAL.CalendarEndOfMonthDate))
					 )
					)
					and convert(varchar(6),CAL.CalendarEndOfMonthDate,112)=@previousmonth
	)
	POL 
	WHERE Rank_record=1
	---pull the latest policy status records per policy key and per month
),
EXP_Collect AS (
	SELECT
	eff_from_date,
	eff_to_date,
	source_sys_id,
	created_date,
	pol_ak_id,
	pol_key,
	pol_eff_date,
	pol_exp_date,
	pol_cancellation_date,
	Pol_compare_date,
	CalendarEndOfMonthDate,
	Rundate,
	pol_cancellation_rsn_code,
	pol_status_code
	FROM SQ_V2Policy_LatestStatusPerMonth
),
LKP_Policy_Dim AS (
	SELECT
	pol_dim_id,
	edw_pol_ak_id,
	eff_from_date,
	eff_to_date
	FROM (
		SELECT 
			pol_dim_id,
			edw_pol_ak_id,
			eff_from_date,
			eff_to_date
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.policy_dim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY edw_pol_ak_id,eff_from_date,eff_to_date ORDER BY pol_dim_id DESC) = 1
),
LKP_ReasonAmendedCodeDim_PMS AS (
	SELECT
	ReasonAmendedCodeDimId,
	ReasonAmendedCode
	FROM (
		SELECT 
			ReasonAmendedCodeDimId,
			ReasonAmendedCode
		FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ReasonAmendedCodeDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ReasonAmendedCode ORDER BY ReasonAmendedCodeDimId DESC) = 1
),
EXP_Policystatus AS (
	SELECT
	EXP_Collect.source_sys_id,
	EXP_Collect.pol_key,
	EXP_Collect.created_date,
	EXP_Collect.eff_from_date,
	EXP_Collect.eff_to_date,
	EXP_Collect.pol_eff_date,
	EXP_Collect.pol_exp_date,
	EXP_Collect.pol_cancellation_date,
	EXP_Collect.Pol_compare_date AS compare_date,
	EXP_Collect.CalendarEndOfMonthDate AS rundate,
	-- *INF*: IIF( TO_CHAR(pol_cancellation_date,'YYYYMMDD')<>'21001231' AND
	-- TO_CHAR(pol_cancellation_date,'YYYYMM')<= TO_CHAR(rundate,'YYYYMM') 
	-- ,'Cancelled',
	-- IIF(TO_CHAR(pol_exp_date,'YYYYMM')<= TO_CHAR(rundate,'YYYYMM') 
	-- ,'Not Inforce',
	-- IIF( TO_CHAR(pol_eff_date,'YYYYMM')>TO_CHAR(rundate,'YYYYMM') 
	-- ,'Future Inforce', 'Inforce')
	-- )
	-- )
	-- 
	-- --Case when convert(varchar(6),pol.pol_cancellation_date,112)='210012'  and
	-- --           convert(varchar(6),pol.pol_exp_date,112)<= convert(varchar(6),CalendarEndOfMonthDate,112) 
	-- --		  then 'Not Inforce' 
	-- --	      when convert(varchar(6),pol.pol_cancellation_date,112)<>'210012' and convert(varchar-(6),pol.pol_cancellation_date,112)<= convert(varchar(6),CalendarEndOfMonthDate,112) 
	-- --     then 'Cancelled' 
	-- --	when convert(varchar(6),pol.pol_eff_date,112)> convert(varchar(6),CalendarEndOfMonthDate,112) 
	-- --	then 'Future Inforce' 
	-- --	else 'Inforce' end Pol_derived_status,
	IFF(
	    TO_CHAR(pol_cancellation_date, 'YYYYMMDD') <> '21001231'
	    and TO_CHAR(pol_cancellation_date, 'YYYYMM') <= TO_CHAR(rundate, 'YYYYMM'),
	    'Cancelled',
	    IFF(
	        TO_CHAR(pol_exp_date, 'YYYYMM') <= TO_CHAR(rundate, 'YYYYMM'), 'Not Inforce',
	        IFF(
	            TO_CHAR(pol_eff_date, 'YYYYMM') > TO_CHAR(rundate, 'YYYYMM'),
	            'Future Inforce',
	            'Inforce'
	        )
	    )
	) AS v_policystatusdescription,
	v_policystatusdescription AS out_policystatusdescription,
	-- *INF*: IIF( TO_CHAR(pol_cancellation_date,'YYYYMMDD')<>'21001231' AND
	-- TO_CHAR(pol_cancellation_date,'YYYYMM')<= TO_CHAR(rundate,'YYYYMM'), 'Cancelled' ,
	-- 
	-- IIF( TO_CHAR(pol_cancellation_date,'YYYYMMDD')<>'21001231' AND
	-- TO_CHAR(pol_cancellation_date,'YYYYMM')> TO_CHAR(rundate,'YYYYMM'),'FutureCancellation',
	-- 
	-- --IIF( TO_CHAR(pol_cancellation_date,'YYYYMMDD')<>'21001231' AND
	-- --TO_CHAR(pol_cancellation_date,'YYYYMM')<= TO_CHAR(ADD_TO_DATE( rundate, 'D', 1 ),'YYYYMM'), 'Cancelled' ,
	-- IIF( TO_CHAR(pol_exp_date,'YYYYMM')<=TO_CHAR(ADD_TO_DATE( rundate, 'D', 1 ),'YYYYMM'), 'Not Inforce',
	-- IIF( TO_CHAR(pol_eff_date,'YYYYMM')>TO_CHAR(ADD_TO_DATE( rundate, 'D', 1 ),'YYYYMM') 
	-- ,'Future Inforce' ,'Inforce')
	-- )
	-- )
	-- )
	-- 
	-- --case when convert(varchar(6),pol.pol_exp_date,112)<= convert(varchar(6),dateadd(dd,1,CalendarEndOfMonthDate),112) 
	-- --	 then 'Not Inforce' 
	-- --	 when pol.pol_cancellation_date<>'2100-12-31 23:59:59' 
	-- --	      and convert(varchar(6),pol.pol_cancellation_date,112)<= convert(varchar(6),dateadd(dd,1,CalendarEndOfMonthDate),112) 
	-- --	 then 'Cancelled' 
	-- --	when convert(varchar(6),pol.pol_eff_date,112)> convert(varchar(6),dateadd(dd,1,CalendarEndOfMonthDate),112) 
	-- --	then 'Future Inforce' 
	-- --	else 'Inforce' end Pol_Future_derived_status,
	IFF(
	    TO_CHAR(pol_cancellation_date, 'YYYYMMDD') <> '21001231'
	    and TO_CHAR(pol_cancellation_date, 'YYYYMM') <= TO_CHAR(rundate, 'YYYYMM'),
	    'Cancelled',
	    IFF(
	        TO_CHAR(pol_cancellation_date, 'YYYYMMDD') <> '21001231'
	        and TO_CHAR(pol_cancellation_date, 'YYYYMM') > TO_CHAR(rundate, 'YYYYMM'),
	        'FutureCancellation',
	        IFF(
	            TO_CHAR(pol_exp_date, 'YYYYMM') <= TO_CHAR(DATEADD(DAY,1,rundate), 'YYYYMM'),
	            'Not Inforce',
	            IFF(
	                TO_CHAR(pol_eff_date, 'YYYYMM') > TO_CHAR(DATEADD(DAY,1,rundate), 'YYYYMM'),
	                'Future Inforce',
	                'Inforce'
	            )
	        )
	    )
	) AS v_futurepolicystatusdescription,
	v_futurepolicystatusdescription AS out_futurepolicystatusdescription,
	-- *INF*: IIF(v_policystatusdescription='Cancelled' ,pol_cancellation_date,
	-- TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	-- )
	-- 
	-- --case when Pol_derived_status='Cancelled' then pol_cancellation_date else '2100-12-31 23:59:59' end pol_cancellation_date,
	IFF(
	    v_policystatusdescription = 'Cancelled', pol_cancellation_date,
	    TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	) AS v_pol_cancellation_date,
	v_pol_cancellation_date AS out_pol_cancellation_date,
	-- *INF*: IIF(IN(v_futurepolicystatusdescription,'FutureCancellation','Cancelled') ,pol_cancellation_date,
	-- TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	-- )
	-- 
	-- --Pol_derived_status,case when Pol_Future_derived_status='Cancelled' then pol_cancellation_date else '2100-12-31 23:59:59' end pol_Future_cancellation_date,
	IFF(
	    v_futurepolicystatusdescription IN ('FutureCancellation','Cancelled'), pol_cancellation_date,
	    TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	) AS v_future_pol_cancellation_date,
	v_future_pol_cancellation_date AS out_future_pol_cancellation_date,
	EXP_Collect.pol_ak_id,
	sessstarttime AS out_created_date,
	LKP_Policy_Dim.pol_dim_id,
	-- *INF*: iif(isnull(pol_dim_id),-1,pol_dim_id)
	IFF(pol_dim_id IS NULL, - 1, pol_dim_id) AS out_pol_dim_id,
	-- *INF*: IIF(IN(v_futurepolicystatusdescription,'FutureCancellation','Cancelled') ,compare_date,
	-- TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	-- )
	-- 
	-- --IIF(v_policystatusdescription='Cancelled' ,i_Cancellation_Enter_date,
	-- --TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	-- --)
	-- 
	-- --case when Pol_derived_status='Cancelled' then Cancellation_Enter_date else '2100-12-31 23:59:59'
	IFF(
	    v_futurepolicystatusdescription IN ('FutureCancellation','Cancelled'), compare_date,
	    TO_TIMESTAMP('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	) AS v_Pol_cancellation_enter_date,
	v_Pol_cancellation_enter_date AS out_Pol_cancellation_enter_date,
	EXP_Collect.pol_status_code,
	EXP_Collect.pol_cancellation_rsn_code,
	LKP_ReasonAmendedCodeDim_PMS.ReasonAmendedCodeDimId AS lkp_ReasonAmendedCodeDimId_pms,
	-- *INF*: iif(source_sys_id='DCT',
	-- iif( isnull(
	-- :LKP.LKP_REASONAMENDEDCODE_DCT(pol_key,TO_CHAR(pol_cancellation_date,'YYYYMMDD'))),-2,
	-- :LKP.LKP_REASONAMENDEDCODE_DCT(pol_key,TO_CHAR(pol_cancellation_date,'YYYYMMDD'))
	-- ))
	-- 
	-- 
	-- 
	-- 
	IFF(
	    source_sys_id = 'DCT',
	    IFF(
	        LKP_REASONAMENDEDCODE_DCT_pol_key_TO_CHAR_pol_cancellation_date_YYYYMMDD.ReasonAmendedCodeDimId IS NULL,
	        - 2,
	        LKP_REASONAMENDEDCODE_DCT_pol_key_TO_CHAR_pol_cancellation_date_YYYYMMDD.ReasonAmendedCodeDimId
	    )
	) AS v_ReasonAmendedCodeDimId_dct,
	-- *INF*:  iif(source_sys_id='PMS' ,
	-- iif(isnull(lkp_ReasonAmendedCodeDimId_pms),-2,
	-- lkp_ReasonAmendedCodeDimId_pms
	-- ))
	-- 
	-- 
	IFF(
	    source_sys_id = 'PMS',
	    IFF(
	        lkp_ReasonAmendedCodeDimId_pms IS NULL, - 2, lkp_ReasonAmendedCodeDimId_pms
	    )
	) AS v_ReasonAmendedCodeDimId_pms,
	-- *INF*: iif(source_sys_id='DCT' and v_policystatusdescription='Cancelled',v_ReasonAmendedCodeDimId_dct,
	-- iif(source_sys_id='PMS' and v_policystatusdescription='Cancelled',v_ReasonAmendedCodeDimId_pms,-1)
	-- )
	-- 
	-- 
	IFF(
	    source_sys_id = 'DCT' and v_policystatusdescription = 'Cancelled',
	    v_ReasonAmendedCodeDimId_dct,
	    IFF(
	        source_sys_id = 'PMS'
	    and v_policystatusdescription = 'Cancelled',
	        v_ReasonAmendedCodeDimId_pms,
	        - 1
	    )
	) AS v_ReasonAmendedCodeDimId,
	v_ReasonAmendedCodeDimId AS out_ReasonAmendedCodeDimId,
	-- *INF*: iif(NOT IN(v_futurepolicystatusdescription,'FutureCancellation','Cancelled') ,-1,
	-- iif(source_sys_id='DCT' ,v_ReasonAmendedCodeDimId_dct,
	-- iif(source_sys_id='PMS',v_ReasonAmendedCodeDimId_pms
	-- )))
	-- 
	-- 
	IFF(
	    NOT v_futurepolicystatusdescription IN ('FutureCancellation','Cancelled'), - 1,
	    IFF(
	        source_sys_id = 'DCT', v_ReasonAmendedCodeDimId_dct,
	        IFF(
	            source_sys_id = 'PMS', v_ReasonAmendedCodeDimId_pms
	        )
	    )
	) AS v_FutureReasonAmendedCodeDimId,
	v_FutureReasonAmendedCodeDimId AS out_FutureReasonAmendedCodeDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM EXP_Collect
	LEFT JOIN LKP_Policy_Dim
	ON LKP_Policy_Dim.edw_pol_ak_id = EXP_Collect.pol_ak_id AND LKP_Policy_Dim.eff_from_date <= EXP_Collect.Rundate AND LKP_Policy_Dim.eff_to_date >= EXP_Collect.Rundate
	LEFT JOIN LKP_ReasonAmendedCodeDim_PMS
	ON LKP_ReasonAmendedCodeDim_PMS.ReasonAmendedCode = EXP_Collect.pol_cancellation_rsn_code
	LEFT JOIN LKP_REASONAMENDEDCODE_DCT LKP_REASONAMENDEDCODE_DCT_pol_key_TO_CHAR_pol_cancellation_date_YYYYMMDD
	ON LKP_REASONAMENDEDCODE_DCT_pol_key_TO_CHAR_pol_cancellation_date_YYYYMMDD.pol_key = pol_key
	AND LKP_REASONAMENDEDCODE_DCT_pol_key_TO_CHAR_pol_cancellation_date_YYYYMMDD.pol_cancellation_date = TO_CHAR(pol_cancellation_date, 'YYYYMMDD')

),
LKP_PolicyCurrentStatusDim AS (
	SELECT
	PolicyCurrentStatusDimId,
	PolicyKey,
	RunDate
	FROM (
		SELECT 
		PolicyCurrentStatusDim.PolicyCurrentStatusDimId as PolicyCurrentStatusDimId,
		 PolicyCurrentStatusDim.PolicyKey as PolicyKey, 
		PolicyCurrentStatusDim.RunDate as RunDate FROM PolicyCurrentStatusDim
		where
		convert(varchar(6),Rundate,112)=convert(varchar(6),dateadd(mm,@{pipeline().parameters.NO_OF_MONTHS},getdate()),112)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,RunDate ORDER BY PolicyCurrentStatusDimId) = 1
),
RTR_INSERT_UPDATE AS (
	SELECT
	LKP_PolicyCurrentStatusDim.PolicyCurrentStatusDimId AS lkp_PolicyCurrentStatusDimId,
	EXP_Policystatus.AuditId,
	EXP_Policystatus.out_created_date AS CreatedDate,
	EXP_Policystatus.out_created_date AS ModifiedDate,
	EXP_Policystatus.out_pol_dim_id AS PolicyDimId,
	EXP_Policystatus.out_ReasonAmendedCodeDimId AS ReasonAmendedCodeDimId,
	EXP_Policystatus.pol_ak_id AS EDWPolicyAKId,
	EXP_Policystatus.pol_key AS PolicyKey,
	EXP_Policystatus.rundate AS RunDate,
	EXP_Policystatus.out_pol_cancellation_date AS PolicyCancellationDate,
	EXP_Policystatus.out_policystatusdescription AS PolicyStatusDescription,
	EXP_Policystatus.out_future_pol_cancellation_date AS PolicyFutureCancellationDate,
	EXP_Policystatus.out_Pol_cancellation_enter_date AS PolicyCancellationEnteredDate,
	EXP_Policystatus.out_futurepolicystatusdescription AS PolicyFutureStatusDescription,
	EXP_Policystatus.out_FutureReasonAmendedCodeDimId AS PolicyFutureCancellationReasonAmendedCodeDimId
	FROM EXP_Policystatus
	LEFT JOIN LKP_PolicyCurrentStatusDim
	ON LKP_PolicyCurrentStatusDim.PolicyKey = EXP_Policystatus.pol_key AND LKP_PolicyCurrentStatusDim.RunDate = EXP_Policystatus.rundate
),
RTR_INSERT_UPDATE_INSERT AS (SELECT * FROM RTR_INSERT_UPDATE WHERE IIF(ISNULL (lkp_PolicyCurrentStatusDimId),  TRUE)),
RTR_INSERT_UPDATE_UPDATE AS (SELECT * FROM RTR_INSERT_UPDATE WHERE IIF(NOT ISNULL (lkp_PolicyCurrentStatusDimId),  TRUE)),
UPD_PolicyCurrentStatusDIm AS (
	SELECT
	lkp_PolicyCurrentStatusDimId AS PolicyCurrentStatusDimId3, 
	AuditId AS AuditId3, 
	ModifiedDate AS ModifiedDate3, 
	PolicyDimId AS PolicyDimId3, 
	ReasonAmendedCodeDimId AS ReasonAmendedCodeDimId3, 
	EDWPolicyAKId AS EDWPolicyAKId3, 
	PolicyKey AS PolicyKey3, 
	RunDate AS RunDate3, 
	PolicyCancellationDate AS PolicyCancellationDate3, 
	PolicyStatusDescription AS PolicyStatusDescription3, 
	PolicyFutureCancellationDate AS PolicyFutureCancellationDate3, 
	PolicyCancellationEnteredDate AS PolicyCancellationEnteredDate3, 
	PolicyFutureStatusDescription AS PolicyFutureStatusDescription3, 
	PolicyFutureCancellationReasonAmendedCodeDimId AS PolicyFutureCancellationReasonAmendedCodeDimId3
	FROM RTR_INSERT_UPDATE_UPDATE
),
PolicyCurrentStatusDimu_UPDATE AS (
	MERGE INTO PolicyCurrentStatusDim AS T
	USING UPD_PolicyCurrentStatusDIm AS S
	ON T.PolicyCurrentStatusDimId = S.PolicyCurrentStatusDimId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId3, T.ModifiedDate = S.ModifiedDate3, T.PolicyDimId = S.PolicyDimId3, T.ReasonAmendedCodeDimId = S.ReasonAmendedCodeDimId3, T.EDWPolicyAKId = S.EDWPolicyAKId3, T.PolicyKey = S.PolicyKey3, T.RunDate = S.RunDate3, T.PolicyCancellationDate = S.PolicyCancellationDate3, T.PolicyStatusDescription = S.PolicyStatusDescription3, T.PolicyFutureCancellationDate = S.PolicyFutureCancellationDate3, T.PolicyCancellationEnteredDate = S.PolicyCancellationEnteredDate3, T.PolicyFutureStatusDescription = S.PolicyFutureStatusDescription3, T.PolicyFutureCancellationReasonAmendedCodeDimId = S.PolicyFutureCancellationReasonAmendedCodeDimId3
),
PolicyCurrentStatusDim_INSERT AS (
	INSERT INTO PolicyCurrentStatusDim
	(AuditId, CreatedDate, ModifiedDate, PolicyDimId, ReasonAmendedCodeDimId, EDWPolicyAKId, PolicyKey, RunDate, PolicyCancellationDate, PolicyStatusDescription, PolicyFutureCancellationDate, PolicyCancellationEnteredDate, PolicyFutureStatusDescription, PolicyFutureCancellationReasonAmendedCodeDimId)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYDIMID, 
	REASONAMENDEDCODEDIMID, 
	EDWPOLICYAKID, 
	POLICYKEY, 
	RUNDATE, 
	POLICYCANCELLATIONDATE, 
	POLICYSTATUSDESCRIPTION, 
	POLICYFUTURECANCELLATIONDATE, 
	POLICYCANCELLATIONENTEREDDATE, 
	POLICYFUTURESTATUSDESCRIPTION, 
	POLICYFUTURECANCELLATIONREASONAMENDEDCODEDIMID
	FROM RTR_INSERT_UPDATE_INSERT
),