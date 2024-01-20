WITH
SQ_EPLI_WorkTable_DCT AS (
	DECLARE @Pol_Inforce_Date datetime, @Pol_Cancelled_date_start datetime, @Pol_Cancelled_date_end datetime 
	
	
	SET @Pol_Inforce_Date =DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE()) + @{pipeline().parameters.NO_OF_MONTHS},0))
	SET @Pol_Cancelled_date_start = DATEADD(month, DATEDIFF(month, 0, GETDATE()) -2 + @{pipeline().parameters.NO_OF_MONTHS}, 0)
	SET @Pol_Cancelled_date_end = DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())-1 + @{pipeline().parameters.NO_OF_MONTHS},0))
	
	
	SELECT DISTINCT rc.ClassCode
		,rc.coveragetype
		,CASE 
			WHEN rc.RatingCoverageCancellationDate BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end
				THEN 'C'
			WHEN rc.RatingCoverageCancellationDate < @Pol_Cancelled_date_start
				AND rc.EffectiveDate < =@Pol_Inforce_Date
				AND rc.EffectiveDate > @Pol_Cancelled_date_end
				THEN 'C'
			ELSE pol.policystatuscode
			END 
		,CASE 
			WHEN rc.RatingCoverageCancellationDate BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end
				THEN 'Cancelled'
			WHEN rc.RatingCoverageCancellationDate < @Pol_Cancelled_date_start
				AND rc.EffectiveDate <= @Pol_Inforce_Date
				AND rc.EffectiveDate > @Pol_Cancelled_date_end
				THEN 'Cancelled'
			ELSE pol.PolicyStatusDescription
			END
		,cc.cust_num
		,pol.pol_key
		,cc.NAME
		,cc.doing_bus_as
		,pt.PremiumTransactionEffectiveDate
		,pt.PremiumTransactionExpirationDate
		,pol.pol_cancellation_date
		,pol.renl_code
		,pol.pol_num
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.contract_customer cc
	JOIN (
		SELECT *
		FROM (
			SELECT pol_mod
				,eff_from_date
				,eff_to_date
				,pol_num
				,pol_key
				,SUBSTRING(pcsd.policystatusdescription, 0, 2) policystatuscode
				,pcsd.PolicyStatusDescription
				,pol_eff_date
				,prior_pol_key
				,pol_exp_date
				,pol_cancellation_date
				,pol.renl_code
				,pol.contract_cust_ak_id
				,crrnt_snpsht_flag
				,pol_ak_id
				,source_sys_id
				,rank() OVER (
					PARTITION BY pol.pol_key ORDER BY pcsd.rundate DESC
					) rnk
			FROM v2.policy pol
			JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCurrentStatusDim pcsd ON pcsd.EDWPolicyAKId = pol.pol_ak_id --and pol.pol_num in ('0054088')
			WHERE pol.crrnt_snpsht_flag = 1
				AND pol_eff_date != pol_cancellation_date
				AND pcsd.PolicyStatusDescription IN (
					'Inforce'
					,'Not Inforce'
					,'Cancelled'
					)
				AND (
					@Pol_Inforce_Date BETWEEN pol_eff_date AND pol_exp_date
					AND pol_cancellation_date > @Pol_Inforce_Date
					OR (
						pol_exp_date BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end
						AND pol_cancellation_date > @Pol_Inforce_Date
						)
					OR (pol_cancellation_date BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end)
					OR (
						pol_cancellation_date < @Pol_Cancelled_date_start
						AND eff_from_date <= @Pol_Inforce_Date
						AND eff_from_date > @Pol_Cancelled_date_end
						) 
					)
			) poli
		WHERE poli.rnk = 1
		) pol ON pol.contract_cust_ak_id = cc.contract_cust_ak_id
		AND cc.crrnt_snpsht_flag = 1
		AND pol.crrnt_snpsht_flag = 1
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.Risklocation rl ON rl.policyAKId = pol.pol_ak_id
		AND rl.CurrentSnapshotFlag = 1
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCoverage pc ON pc.RiskLocationAKID = rl.RiskLocationAKID
		AND pc.CurrentSnapshotFlag = 1
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage rc ON rc.PolicyCoverageAKID = pc.PolicyCoverageAKID
		AND rc.CurrentSnapshotFlag = 1
	JOIN PremiumTransaction pt ON pt.RatingCoverageAKID = rc.RatingCoverageAKID
		AND pt.EffectiveDate = rc.EffectiveDate
	JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceLineOfBusiness ir ON rc.InsuranceReferenceLineOfBusinessAKID = ir.InsuranceReferenceLineOfBusinessAKID
	WHERE (
			(
				pc.InsuranceLine = 'EmploymentPracticesLiab'
				AND rc.coveragetype = 'EmploymentPracticesLiability'
				)
			OR (
				(
					rc.coveragetype LIKE 'WB516%'
					OR rc.coveragetype = 'NS0279'
					OR rc.coveragetype = 'NS0313'
					)
				AND (
					rc.ClassCode = '04033'
					OR rc.ClassCode = '7070'
					)
				)
			OR (
				pc.InsuranceLine = 'Businessowners'
				AND rc.CoverageType = 'EmploymentPracticesLiability'
				)
			)
		AND (
			(
				pol.PolicyStatusDescription = 'Inforce'
				AND rc.RatingCoverageEffectiveDate != rc.RatingCoverageCancellationDate
				AND (
					rc.RatingCoverageCancellationDate BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end
					OR (rc.RatingCoverageCancellationDate < @Pol_Cancelled_date_start
					AND rc.EffectiveDate <= @Pol_Inforce_Date
					AND rc.EffectiveDate > @Pol_Cancelled_date_end)
					OR rc.RatingCoverageCancellationDate > @Pol_Inforce_Date
					)
				--AND pt.PremiumTransactionEffectiveDate <= @Pol_Inforce_Date permanent comment
				)
			OR (
				pol.PolicyStatusDescription = 'Not Inforce'
				AND pol.pol_exp_date BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end
		             AND rc.RatingCoverageCancellationDate >= @Pol_Cancelled_date_start
				)
			OR (
				pol.PolicyStatusDescription = 'Cancelled'
				AND pt.PremiumTransactionEffectiveDate <= @Pol_Inforce_Date
				AND pol.pol_cancellation_date BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end
	                   AND rc.RatingCoverageCancellationDate >= @Pol_Cancelled_date_start
				)
			OR (
				pol.PolicyStatusDescription = 'Cancelled'
				AND pol.eff_from_date > @Pol_Cancelled_date_end
				AND pol.eff_from_date <= @Pol_Inforce_Date
				AND pol.pol_cancellation_date < @Pol_Cancelled_date_start
				)
			)
	ORDER BY 6,4
),
EXP_GetValues AS (
	SELECT
	ClassCode,
	-- *INF*: SUBSTR(ClassCode,1,3)
	SUBSTR(ClassCode, 1, 3) AS o_ClassCode,
	InsuranceLine,
	StandardPolicyStatusCode,
	StandardPolicyStatusCodeDescription,
	cust_num,
	pol_key,
	name,
	doing_bus_as,
	EffectiveDate,
	ExpirationDate,
	-- *INF*: -- TRUNC(@{pipeline().parameters.EXTRACTDATE},'D')
	-- 
	-- LAST_DAY(Add_to_date(@{pipeline().parameters.EXTRACTDATE}, 'MONTH' , -1))
	LAST_DAY(DATEADD(MONTH,- 1,@{pipeline().parameters.EXTRACTDATE})) AS o_EXTRACT_DATE,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SOURCE_SYSTEM_ID,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AUDIT_ID,
	pol_cancellation_date AS Pol_Cancellation_date,
	renl_code AS Renl_code,
	pol_num
	FROM SQ_EPLI_WorkTable_DCT
),
AGG_KeyFeilds AS (
	SELECT
	o_ClassCode,
	InsuranceLine,
	StandardPolicyStatusCode,
	StandardPolicyStatusCodeDescription,
	cust_num,
	pol_num,
	pol_key,
	name,
	doing_bus_as,
	EffectiveDate,
	ExpirationDate,
	o_EXTRACT_DATE,
	o_SOURCE_SYSTEM_ID,
	o_AUDIT_ID,
	Pol_Cancellation_date,
	Renl_code
	FROM EXP_GetValues
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLine, cust_num, pol_num ORDER BY NULL) = 1
),
lkp_Prior_Rewrite_pol_check AS (
	SELECT
	Prior_pol_key,
	pol_key,
	pol_key_in
	FROM (
		DECLARE @Pol_Inforce_Date datetime, @Pol_Cancelled_date_start datetime, @Pol_Cancelled_date_end datetime 
		
		
		SET @Pol_Inforce_Date =DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE()) + @{pipeline().parameters.NO_OF_MONTHS},0))
		SET @Pol_Cancelled_date_start = DATEADD(month, DATEDIFF(month, 0, GETDATE()) -2 + @{pipeline().parameters.NO_OF_MONTHS}, 0)
		SET @Pol_Cancelled_date_end = DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,GETDATE())-1 + @{pipeline().parameters.NO_OF_MONTHS},0))
		
		SELECT A.Prior_Pol_key AS Prior_pol_key
			,A.pol_key AS pol_key
		FROM (
			SELECT DISTINCT P.Pol_key Prior_Pol_key
				,A.pol_key
				,PriorExpirationDate
				,Row_number() OVER (
					PARTITION BY RC.CoverageGUID ORDER BY RC.EffectiveDate
					) Record_Count
				,A.Rewrite_Record_Count
			FROM v2.policy P
			INNER JOIN RiskLocation R ON P.pol_ak_id = R.PolicyAKID
				AND P.source_sys_id = 'DCT'
				AND R.SourceSystemID = 'DCT'
				AND P.crrnt_snpsht_flag = 1
				AND R.CurrentSnapshotFlag = 1
			INNER JOIN Policycoverage PC ON R.RiskLocationAKID = PC.RiskLocationAKID
				AND PC.SourceSystemID = 'DCT'
				AND PC.CurrentSnapshotFlag = 1
			INNER JOIN RatingCoverage RC ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
			INNER JOIN PremiumTransaction PT ON RC.RatingCoverageAKID = PT.RatingCoverageAKId
				AND RC.EffectiveDate = PT.EffectiveDate
				AND PT.SourceSystemID = 'DCT'
			INNER JOIN (
				SELECT DISTINCT P.Pol_key
					,P.pol_ak_id
					,RC.EffectiveDate
					,dateadd(S, - 1, RC.EffectiveDate) PriorExpirationDate
					,RC.CoverageGUID
					,PT.PremiumTransactionCode
					,Row_number() OVER (
						PARTITION BY RC.CoverageGUID ORDER BY RC.EffectiveDate
						) Rewrite_Record_Count
				FROM (
					SELECT *
					FROM (
						SELECT pol_mod
							,eff_from_date
							,eff_to_date
							,pol_num
							,pol_key
							,SUBSTRING(pcsd.policystatusdescription, 0, 2) policystatuscode
							,pcsd.PolicyStatusDescription
							,pol_eff_date
							,prior_pol_key
							,pol_exp_date
							,pol_cancellation_date
							,pol.renl_code
							,pol.contract_cust_ak_id
							,crrnt_snpsht_flag
							,pol_ak_id
							,source_sys_id
							,rank() OVER (
								PARTITION BY pol.pol_key ORDER BY pcsd.rundate DESC
								) rnk
						FROM v2.policy pol
						JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.PolicyCurrentStatusDim pcsd ON pcsd.EDWPolicyAKId = pol.pol_ak_id
						WHERE pol.crrnt_snpsht_flag = 1
							AND pol_eff_date != pol_cancellation_date
							AND pcsd.PolicyStatusDescription IN (
								'Inforce'
								,'Not Inforce'
								,'Cancelled'
								)
							AND (
								@Pol_Inforce_Date BETWEEN pol_eff_date AND pol_exp_date
								AND pol_cancellation_date > @Pol_Inforce_Date
								OR (
									pol_exp_date BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end
									AND pol_cancellation_date > @Pol_Inforce_Date
									)
								OR (pol_cancellation_date BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end)
								OR (
									pol_cancellation_date < @Pol_Cancelled_date_start
									AND pol_eff_date < = @Pol_Inforce_Date
									AND pol_eff_date > @Pol_Cancelled_date_end
									)
								)
						) poli
					WHERE poli.rnk = 1
					) P
				INNER JOIN RiskLocation R ON P.pol_ak_id = R.PolicyAKID
					AND P.source_sys_id = 'DCT'
					AND R.SourceSystemID = 'DCT'
					AND P.crrnt_snpsht_flag = 1
					AND R.CurrentSnapshotFlag = 1
				INNER JOIN Policycoverage PC ON R.RiskLocationAKID = PC.RiskLocationAKID
					AND PC.SourceSystemID = 'DCT'
					AND PC.CurrentSnapshotFlag = 1
				INNER JOIN RatingCoverage RC ON PC.PolicyCoverageAKID = RC.PolicyCoverageAKID
					AND RC.CurrentSnapshotFlag = 1
				INNER JOIN PremiumTransaction PT ON RC.RatingCoverageAKID = PT.RatingCoverageAKId
					AND RC.EffectiveDate = PT.EffectiveDate
					AND PT.SourceSystemID = 'DCT'
				WHERE PT.OffsetOnsetCode = 'N/A'
					AND (
						(
							pc.InsuranceLine = 'EmploymentPracticesLiab'
							AND rc.coveragetype = 'EmploymentPracticesLiability'
							)
						OR (
							(
								rc.coveragetype LIKE 'WB516%'
								OR rc.coveragetype = 'NS0279'
								OR rc.coveragetype = 'NS0313'
								)
							AND (
								rc.ClassCode = '04033'
								OR rc.ClassCode = '7070'
								)
							)
						OR (
							pc.InsuranceLine = 'Businessowners'
							AND rc.CoverageType = 'EmploymentPracticesLiability'
							)
						)
					AND (
						(
							p.PolicyStatusDescription = 'Inforce'
							AND RC.RatingCoverageEffectiveDate != RC.RatingCoverageCancellationDate
							AND (
								rc.RatingCoverageCancellationDate BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end
								OR (
									rc.RatingCoverageCancellationDate < @Pol_Cancelled_date_start
									AND rc.EffectiveDate <= @Pol_Inforce_Date
									AND rc.EffectiveDate > @Pol_Cancelled_date_end
									)
								OR rc.RatingCoverageCancellationDate > @Pol_Inforce_Date
								)
							)
						OR (
							p.PolicyStatusDescription = 'Not Inforce'
							AND p.pol_exp_date BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end
							)
						OR (
							p.PolicyStatusDescription = 'Cancelled'
							AND pt.PremiumTransactionEffectiveDate < = @Pol_Inforce_Date
							AND p.pol_cancellation_date BETWEEN @Pol_Cancelled_date_start AND @Pol_Cancelled_date_end
							)
						OR (
							p.PolicyStatusDescription = 'Cancelled'
							AND p.eff_from_date > @Pol_Cancelled_date_end
							AND p.eff_from_date <= @Pol_Inforce_Date
							AND p.pol_cancellation_date < @Pol_Cancelled_date_start
							)
						)
				) A ON RC.CoverageGUID = A.CoverageGUID
			--and RC.ExpirationDate=PriorExpirationDate
			WHERE A.PremiumTransactionCode = 'Rewrite'
			) A
		WHERE Rewrite_Record_Count - 1 = Record_Count--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Prior_pol_key ORDER BY Prior_pol_key) = 1
),
FIL_ExcessRecords AS (
	SELECT
	AGG_KeyFeilds.o_ClassCode AS ClassCode, 
	AGG_KeyFeilds.InsuranceLine, 
	AGG_KeyFeilds.StandardPolicyStatusCode, 
	AGG_KeyFeilds.StandardPolicyStatusCodeDescription, 
	AGG_KeyFeilds.cust_num, 
	AGG_KeyFeilds.pol_key, 
	AGG_KeyFeilds.name, 
	AGG_KeyFeilds.doing_bus_as, 
	AGG_KeyFeilds.EffectiveDate, 
	AGG_KeyFeilds.ExpirationDate, 
	AGG_KeyFeilds.FullTermPremium, 
	AGG_KeyFeilds.o_EXTRACT_DATE AS EXTRACT_DATE, 
	AGG_KeyFeilds.o_SOURCE_SYSTEM_ID AS SOURCE_SYSTEM_ID, 
	AGG_KeyFeilds.o_AUDIT_ID AS AUDIT_ID, 
	AGG_KeyFeilds.Pol_Cancellation_date, 
	AGG_KeyFeilds.Renl_code, 
	lkp_Prior_Rewrite_pol_check.Prior_pol_key
	FROM AGG_KeyFeilds
	LEFT JOIN lkp_Prior_Rewrite_pol_check
	ON lkp_Prior_Rewrite_pol_check.Prior_pol_key = AGG_KeyFeilds.pol_key
	WHERE IIF(Prior_pol_key!=pol_key,true,false)
),
EXP_Changes AS (
	SELECT
	ClassCode,
	InsuranceLine,
	StandardPolicyStatusCode AS i_StandardPolicyStatusCode,
	StandardPolicyStatusCodeDescription AS i_StandardPolicyStatusCodeDescription,
	cust_num,
	pol_key,
	name,
	doing_bus_as,
	EffectiveDate,
	ExpirationDate,
	EXTRACT_DATE,
	SOURCE_SYSTEM_ID,
	AUDIT_ID,
	Pol_Cancellation_date,
	Renl_code
	FROM FIL_ExcessRecords
),
TGT_WorkIn2vateEPLI_Insert AS (

	------------ PRE SQL ----------
	@{pipeline().parameters.DELETE_PRESQL}
	-------------------------------


	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.WorkIn2vateEPLI
	(ClassCode, InsuranceLineCode, PolStatusCode, PolStatusDescription, CustomerNumber, PolicyKey, Name, DoingBusinessAs, PremiumTransactionEffectiveDate, PremiumTransactionExpirationDate, ExtractDate, SourceSystemId, AuditID)
	SELECT 
	CLASSCODE, 
	InsuranceLine AS INSURANCELINECODE, 
	i_StandardPolicyStatusCode AS POLSTATUSCODE, 
	i_StandardPolicyStatusCodeDescription AS POLSTATUSDESCRIPTION, 
	cust_num AS CUSTOMERNUMBER, 
	pol_key AS POLICYKEY, 
	name AS NAME, 
	doing_bus_as AS DOINGBUSINESSAS, 
	EffectiveDate AS PREMIUMTRANSACTIONEFFECTIVEDATE, 
	ExpirationDate AS PREMIUMTRANSACTIONEXPIRATIONDATE, 
	EXTRACT_DATE AS EXTRACTDATE, 
	SOURCE_SYSTEM_ID AS SOURCESYSTEMID, 
	AUDIT_ID AS AUDITID
	FROM EXP_Changes
),