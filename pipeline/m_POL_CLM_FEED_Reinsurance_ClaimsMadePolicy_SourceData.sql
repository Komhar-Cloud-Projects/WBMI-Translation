WITH
LKP_Get_Max_Sapiens_SourceSequenceNumber AS (
	SELECT
	Source_Seq_Num,
	ID
	FROM (
		SELECT MAX(A.SourceSequenceNumber) AS Source_Seq_Num,
			1 AS ID
		FROM (
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsurancePolicy
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicy
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsuranceClaim
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceClaim
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsuranceClaimRestate
			UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceClaimRestate
		       UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsurancePolicyRestate
		       UNION ALL
			SELECT isnull(max(SourceSequenceNumber), 999) AS SourceSequenceNumber
			FROM @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsurancePolicyRestate	) A
			--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ID ORDER BY Source_Seq_Num DESC) = 1
),
SQ_SapiensReinsuranceHeaderExtract AS (
	SELECT DISTINCT RTRIM(A.POLICY_NO) as POLICY_NO,
		RTRIM(AccountingProductCode) as AccountingProductCode,
		RTRIM(AnnualStatementLineCode) as AnnualStatementLineCode,
		RTRIM(CompanyCode) as CompanyCode,
		RTRIM(LineOfBusiness) as LineOfBusiness,
		RTRIM(StrategicProfitCenter) as StrategicProfitCenter,
		RTRIM(ProductCode) as ProductCode,
		RTRIM(RiskState) as RiskState,
		ISNULL(RTRIM(SubASLCode), '0') as SubASLCode,
		ISNULL(RTRIM(ReinsuranceUmbrellaLayer), '0') as ReinsuranceUmbrellaLayer
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.SapiensReinsuranceHeaderExtract A
	INNER JOIN (
		SELECT SOURCE_SEQ_NUM,
			ACP AccountingProductCode,
			ASL AnnualStatementLineCode,
			COM CompanyCode,
			INM InsuredName,
			LOB LineOfBusiness,
			PCN StrategicProfitCenter,
			PDT ProductCode,
			RKS RiskState,
			SAS SubASLCode,
			ZRP RatingPlanAbbrevation,
			SNA ReinsuranceUmbrellaLayer
		FROM (
			SELECT SOURCE_SEQ_NUM,
				ATTR_CODE,
				ATTR_VAL
			FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.SapiensReinsuranceAttributesExtract
			WHERE AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
			) C
		pivot(max(ATTR_VAL) FOR ATTR_CODE IN (ACP, ASL, COM, INM, LOB, PCN, PDT, RKS, SAS, ZRP, SNA)) PV
		) B ON A.SOURCE_SEQ_NUM = B.SOURCE_SEQ_NUM
		AND A.AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	WHERE DATA_SOURCE = 'SRL' 
	@{pipeline().parameters.FILTER_CLAUSE} 
	@{pipeline().parameters.WHERE_CLAUSE}
),
LKP_Existing_Policy AS (
	SELECT
	POLICY_NO,
	AccountingProductCode,
	AnnualStatementLineCode,
	CompanyCode,
	LineOfBusiness,
	StrategicProfitCenter,
	ProductCode,
	RiskState,
	SubASLCode,
	ReinsuranceUmbrellaLayer
	FROM (
		SELECT RTRIM(A.POLICY_NO) AS POLICY_NO,
			RTRIM(AccountingProductCode) AS AccountingProductCode,
			RTRIM(AnnualStatementLineCode) AS AnnualStatementLineCode,
			RTRIM(CompanyCode) AS CompanyCode,
			RTRIM(InsuredName) AS InsuredName,
			RTRIM(LineOfBusiness) AS LineOfBusiness,
			RTRIM(StrategicProfitCenter) AS StrategicProfitCenter,
			RTRIM(ProductCode) AS ProductCode,
			RTRIM(RiskState) AS RiskState,
			ISNULL(RTRIM(SubASLCode), '0') AS SubASLCode,
			RTRIM(RatingPlanAbbrevation) AS RatingPlanAbbrevation,
			ISNULL(RTRIM(ReinsuranceUmbrellaLayer), '0') AS ReinsuranceUmbrellaLayer
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract A
		INNER JOIN (
			SELECT SOURCE_SEQ_NUM,
				ACP AccountingProductCode,
				ASL AnnualStatementLineCode,
				COM CompanyCode,
				INM InsuredName,
				LOB LineOfBusiness,
				PCN StrategicProfitCenter,
				PDT ProductCode,
				RKS RiskState,
				SAS SubASLCode,
				ZRP RatingPlanAbbrevation,
				SNA ReinsuranceUmbrellaLayer
			FROM (
				SELECT SOURCE_SEQ_NUM,
					ATTR_CODE,
					ATTR_VAL
				FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchSapiensReinsuranceAttributesExtract
				) Z
			pivot(max(ATTR_VAL) FOR ATTR_CODE IN (ACP, ASL, COM, INM, LOB, PCN, PDT, RKS, SAS, ZRP, SNA)) PV
			) B ON A.SOURCE_SEQ_NUM = B.SOURCE_SEQ_NUM
		INNER JOIN (
			SELECT POLICY_NO,
				max(CASE WHEN DOCUMENT_TYPE = 'N' THEN SOURCE_SEQ_NUM ELSE 0 END) max_SOURCE_SEQ_NUM
			FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract E
			WHERE DATA_SOURCE = 'SRP'
			GROUP BY POLICY_NO
			) D ON A.POLICY_NO = D.POLICY_NO
			AND A.SOURCE_SEQ_NUM > max_SOURCE_SEQ_NUM
		WHERE DATA_SOURCE = 'SRP'
			AND DOCUMENT_TYPE = 'P'
			AND EXISTS (
				SELECT 1
				FROM (
					SELECT POLICY_NO
					FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract
					WHERE DATA_SOURCE = 'SRL'
						AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
					UNION
					SELECT POLICY_NO
					FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.SapiensReinsuranceHeaderExtract
					WHERE DATA_SOURCE = 'SRL'
						AND AuditId = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
					) C
				WHERE A.POLICY_NO = C.POLICY_NO
				)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY POLICY_NO,AccountingProductCode,AnnualStatementLineCode,CompanyCode,LineOfBusiness,StrategicProfitCenter,ProductCode,RiskState,SubASLCode,ReinsuranceUmbrellaLayer ORDER BY POLICY_NO DESC) = 1
),
EXP_SrcData AS (
	SELECT
	LKP_Existing_Policy.POLICY_NO AS LKP_POLICY_NO,
	SQ_SapiensReinsuranceHeaderExtract.POLICY_NO,
	-- *INF*: LTRIM(RTRIM(POLICY_NO))
	LTRIM(RTRIM(POLICY_NO)) AS O_POLICY_NO,
	SQ_SapiensReinsuranceHeaderExtract.AccountingProductCode,
	SQ_SapiensReinsuranceHeaderExtract.AnnualStatementLineCode,
	SQ_SapiensReinsuranceHeaderExtract.CompanyCode,
	SQ_SapiensReinsuranceHeaderExtract.LineOfBusiness,
	SQ_SapiensReinsuranceHeaderExtract.StrategicProfitCenter,
	SQ_SapiensReinsuranceHeaderExtract.ProductCode,
	SQ_SapiensReinsuranceHeaderExtract.RiskState,
	SQ_SapiensReinsuranceHeaderExtract.SubASLCode,
	SQ_SapiensReinsuranceHeaderExtract.ReinsuranceUmbrellaLayer
	FROM SQ_SapiensReinsuranceHeaderExtract
	LEFT JOIN LKP_Existing_Policy
	ON LKP_Existing_Policy.POLICY_NO = SQ_SapiensReinsuranceHeaderExtract.POLICY_NO AND LKP_Existing_Policy.AccountingProductCode = SQ_SapiensReinsuranceHeaderExtract.AccountingProductCode AND LKP_Existing_Policy.AnnualStatementLineCode = SQ_SapiensReinsuranceHeaderExtract.AnnualStatementLineCode AND LKP_Existing_Policy.CompanyCode = SQ_SapiensReinsuranceHeaderExtract.CompanyCode AND LKP_Existing_Policy.LineOfBusiness = SQ_SapiensReinsuranceHeaderExtract.LineOfBusiness AND LKP_Existing_Policy.StrategicProfitCenter = SQ_SapiensReinsuranceHeaderExtract.StrategicProfitCenter AND LKP_Existing_Policy.ProductCode = SQ_SapiensReinsuranceHeaderExtract.ProductCode AND LKP_Existing_Policy.RiskState = SQ_SapiensReinsuranceHeaderExtract.RiskState AND LKP_Existing_Policy.SubASLCode = SQ_SapiensReinsuranceHeaderExtract.SubASLCode AND LKP_Existing_Policy.ReinsuranceUmbrellaLayer = SQ_SapiensReinsuranceHeaderExtract.ReinsuranceUmbrellaLayer
),
FIL_Existing_PoliciesForClaims AS (
	SELECT
	LKP_POLICY_NO, 
	O_POLICY_NO AS POLICY_NO, 
	AccountingProductCode, 
	AnnualStatementLineCode, 
	CompanyCode, 
	LineOfBusiness, 
	StrategicProfitCenter, 
	ProductCode, 
	RiskState, 
	SubASLCode, 
	ReinsuranceUmbrellaLayer
	FROM EXP_SrcData
	WHERE ISNULL(LKP_POLICY_NO)
),
LKP_Dates AS (
	SELECT
	pol_eff_date,
	RatingPlanAbbreviation,
	InsuredName,
	pol_exp_date,
	pol_key
	FROM (
		select pol_key as pol_key,case when Min_RatingPlanAbbreviation is null or Min_RatingPlanAbbreviation='N/A' then max_RatingPlanAbbreviation else Min_RatingPlanAbbreviation end as RatingPlanAbbreviation,InsuredName as InsuredName,pol_eff_date as pol_eff_date,pol_exp_date as pol_exp_date from (
		select pol_key,min(RatingPlanAbbreviation) Min_RatingPlanAbbreviation,max(RatingPlanAbbreviation) max_RatingPlanAbbreviation,E.name InsuredName,pol_eff_date,pol_exp_date from @{pipeline().parameters.LKP_DATABASE_NAME}.@{pipeline().parameters.LKP_TABLE_OWNER}.claim_loss_transaction_fact A
		inner join @{pipeline().parameters.LKP_DATABASE_NAME}.@{pipeline().parameters.LKP_TABLE_OWNER}.policy_dim B
		on A.pol_dim_id=B.pol_dim_id
		inner join @{pipeline().parameters.LKP_DATABASE_NAME}.@{pipeline().parameters.LKP_TABLE_OWNER}.InsuranceReferenceDim C
		on A.InsuranceReferenceDimId=C.InsuranceReferenceDimId
		inner join @{pipeline().parameters.LKP_DATABASE_NAME}.@{pipeline().parameters.LKP_TABLE_OWNER}.calendar_dim D
		on A.claim_trans_date_id=D.clndr_id
		inner join @{pipeline().parameters.LKP_DATABASE_NAME}.@{pipeline().parameters.LKP_TABLE_OWNER}.VWContractCustomerDim E
		on A.contract_cust_dim_id=E.contract_cust_dim_id
		group by pol_key,pol_eff_date,pol_exp_date,E.name) A
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_eff_date DESC) = 1
),
EXP_Defaults AS (
	SELECT
	LKP_Dates.pol_eff_date,
	LKP_Dates.pol_exp_date,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	CURRENT_TIMESTAMP AS CreatedDate,
	CURRENT_TIMESTAMP AS ModifiedDate,
	FIL_Existing_PoliciesForClaims.POLICY_NO AS PolicyKey,
	'P' AS DocumntType,
	pol_eff_date AS AccountingDate,
	0.0 AS MonthlyTotalDirectWrittenPremium,
	FIL_Existing_PoliciesForClaims.ProductCode,
	FIL_Existing_PoliciesForClaims.StrategicProfitCenter AS StrategicProfitCenterAbbreviation,
	FIL_Existing_PoliciesForClaims.AccountingProductCode,
	FIL_Existing_PoliciesForClaims.LineOfBusiness AS InsuranceReferenceLineOfBusinessCode,
	FIL_Existing_PoliciesForClaims.AnnualStatementLineCode AS ASLCode,
	FIL_Existing_PoliciesForClaims.SubASLCode,
	-- *INF*: IIF(ltrim(rtrim(SubASLCode))='0',NULL,ltrim(rtrim(SubASLCode)))
	IFF(ltrim(rtrim(SubASLCode)) = '0', NULL, ltrim(rtrim(SubASLCode))) AS v_SubASLCode,
	v_SubASLCode AS O_SubASLCode,
	FIL_Existing_PoliciesForClaims.RiskState AS PrimaryStateCode,
	pol_eff_date AS CoverageEffectiveDate,
	pol_exp_date AS CoverageExpirationDate,
	pol_eff_date AS EndorsementStartDate,
	pol_eff_date AS EndorsementIssueDate,
	pol_eff_date AS PolicyIssueDate,
	v_count+1 AS v_count,
	-- *INF*: IIF(ISNULL(:LKP.LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER(1)),v_count,:LKP.LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER(1)+v_count)
	IFF(
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num IS NULL, v_count,
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num + v_count
	) AS SourceSequenceNumber,
	2 AS TransactionNumber,
	0 AS EndorsementNumber,
	-- *INF*: MD5(PolicyKey||'|'||ProductCode||'|'||StrategicProfitCenterAbbreviation||'|'||AccountingProductCode||'|'||InsuranceReferenceLineOfBusinessCode||'|'||ASLCode||'|'||v_SubASLCode||'|'||PrimaryStateCode||'|'||to_char(ReinsuranceUmbrellaLayer))
	MD5(PolicyKey || '|' || ProductCode || '|' || StrategicProfitCenterAbbreviation || '|' || AccountingProductCode || '|' || InsuranceReferenceLineOfBusinessCode || '|' || ASLCode || '|' || v_SubASLCode || '|' || PrimaryStateCode || '|' || to_char(ReinsuranceUmbrellaLayer)) AS ASLCoversKey,
	-- *INF*: md5(to_char(pol_eff_date,'YYYYMMDD')||'|'||to_char(pol_exp_date,'YYYYMMDD'))
	md5(to_char(pol_eff_date, 'YYYYMMDD') || '|' || to_char(pol_exp_date, 'YYYYMMDD')) AS DateKey,
	FIL_Existing_PoliciesForClaims.ReinsuranceUmbrellaLayer,
	-- *INF*: IIF(ReinsuranceUmbrellaLayer=-1,NULL,ReinsuranceUmbrellaLayer)
	IFF(ReinsuranceUmbrellaLayer = - 1, NULL, ReinsuranceUmbrellaLayer) AS O_ReinsuranceUmbrellaLayer,
	'' AS OSECode,
	'CLAIMSMADE' AS EntryProcess,
	LKP_Dates.RatingPlanAbbreviation,
	LKP_Dates.InsuredName AS FirstNameIsured
	FROM FIL_Existing_PoliciesForClaims
	LEFT JOIN LKP_Dates
	ON LKP_Dates.pol_key = FIL_Existing_PoliciesForClaims.POLICY_NO
	LEFT JOIN LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1
	ON LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.ID = 1

),
SapiensReinsurancePolicy AS (
	TRUNCATE TABLE SapiensReinsurancePolicy;
	INSERT INTO SapiensReinsurancePolicy
	(AuditId, CreatedDate, ModifiedDate, PolicyKey, DocumntType, AccountingDate, MonthlyTotalDirectWrittenPremium, ProductCode, StrategicProfitCenterAbbreviation, AccountingProductCode, InsuranceReferenceLineOfBusinessCode, ASLCode, SubASLCode, PrimaryStateCode, CoverageEffectiveDate, CoverageExpirationDate, EndorsementStartDate, EndorsementIssueDate, PolicyIssueDate, SourceSequenceNumber, TransactionNumber, EndorsementNumber, ASLCoversKey, DateKey, ReinsuranceUmbrellaLayer, OSECode, EntryProcess, RatingPlanAbbreviation, FirstNameIsured)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYKEY, 
	DOCUMNTTYPE, 
	ACCOUNTINGDATE, 
	MONTHLYTOTALDIRECTWRITTENPREMIUM, 
	PRODUCTCODE, 
	STRATEGICPROFITCENTERABBREVIATION, 
	ACCOUNTINGPRODUCTCODE, 
	INSURANCEREFERENCELINEOFBUSINESSCODE, 
	ASLCODE, 
	O_SubASLCode AS SUBASLCODE, 
	PRIMARYSTATECODE, 
	COVERAGEEFFECTIVEDATE, 
	COVERAGEEXPIRATIONDATE, 
	ENDORSEMENTSTARTDATE, 
	ENDORSEMENTISSUEDATE, 
	POLICYISSUEDATE, 
	SOURCESEQUENCENUMBER, 
	TRANSACTIONNUMBER, 
	ENDORSEMENTNUMBER, 
	ASLCOVERSKEY, 
	DATEKEY, 
	O_ReinsuranceUmbrellaLayer AS REINSURANCEUMBRELLALAYER, 
	OSECODE, 
	ENTRYPROCESS, 
	RATINGPLANABBREVIATION, 
	FIRSTNAMEISURED
	FROM EXP_Defaults
),