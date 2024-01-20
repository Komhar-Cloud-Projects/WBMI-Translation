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
SQ_claim_occurrence_dim AS (
	select * 
	from (SELECT 
		CASE WHEN cod.claim_num = 'N/A' THEN RTRIM(cod.claim_occurrence_key) ELSE RTRIM(cod.claim_num) END as ClaimNumber,
		P.pol_key as PolicyKey,
		cfint.financial_type_code as FinancialTypeCode,
		transdt.CalendarDate as ClaimTransactionDate,
		ctyp.trans_code as TransactionCode,
		CASE row_number() OVER (PARTITION BY cod.claim_num, P.pol_key, IRD.ProductCode, IRD.AccountingProductCode, IRD.StrategicProfitCenterAbbreviation, asld.asl_code, asld.sub_asl_code, IRD.InsuranceReferenceLineOfBusinessCode, P.state_of_domicile_code, cov.edw_claimant_cov_det_ak_id, cfint.financial_type_code
				ORDER BY cltf.claim_trans_date_id desc, cltf.edw_claim_trans_pk_id desc) 
			WHEN 1 THEN 1
			ELSE 0
		END as ContainsOutstandingReserveAmountFlag
	
		FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_loss_transaction_fact cltf with (nolock) 
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P with (nolock) ON cltf.pol_dim_id = P.pol_dim_id
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction_type_dim ctyp with (nolock) ON cltf.claim_trans_type_dim_id = ctyp.claim_trans_type_dim_id 
			AND ctyp.trans_kind_code = 'D'
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_financial_type_dim cfint with (nolock) ON cfint.claim_financial_type_dim_id = cltf.claim_financial_type_dim_id
			AND cfint.financial_type_code in ('D','E','S','R','B')
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim cod with (nolock) ON cltf.claim_occurrence_dim_id = cod.claim_occurrence_dim_id
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_dim clmt with (nolock) ON cltf.claimant_dim_id = clmt.claimant_dim_id
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_dim cov with (nolock) ON cltf.claimant_cov_dim_id = cov.claimant_cov_dim_id
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.InsuranceReferenceDim IRD with (nolock) ON cltf.InsuranceReferenceDimId = IRD.InsuranceReferenceDimId
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.asl_dim asld with (nolock) ON cltf.asl_dim_id = asld.asl_dim_id
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.calendar_dim transdt with (nolock) ON transdt.clndr_id = cltf.claim_trans_date_id
		@{pipeline().parameters.WHERE}
	) A 
	where A.ClaimTransactionDate >= CAST('@{pipeline().parameters.SELECTION_START_TS}' as date)
),
EXP_Collect AS (
	SELECT
	ClaimNumber AS i_ClaimNumber,
	-- *INF*: LTRIM(RTRIM(i_ClaimNumber))
	LTRIM(RTRIM(i_ClaimNumber)) AS o_ClaimNumber,
	PolicyKey,
	FinancialTypeCode,
	ClaimTransactionDate,
	TransactionCode,
	ContainsOutstandingReserveAmountFlag
	FROM SQ_claim_occurrence_dim
),
FIL_Only_Reqd_Transactions AS (
	SELECT
	o_ClaimNumber AS ClaimNumber, 
	PolicyKey, 
	FinancialTypeCode, 
	ClaimTransactionDate, 
	TransactionCode, 
	ContainsOutstandingReserveAmountFlag
	FROM EXP_Collect
	WHERE --Reserves
(
    (INDEXOF(TransactionCode,'40','41','42','65','66','90','91','92','95','97','98','99') > 0 
    or ContainsOutstandingReserveAmountFlag='1') 
    AND 
    INDEXOF(FinancialTypeCode,'D','E') > 0
)
OR
-- Payments
(
    INDEXOF(TransactionCode,'40','41','42','65','66','90','91','92','95','97','98','99')=0
)
),
AGG_OneRowPerClaim AS (
	SELECT
	ClaimNumber,
	PolicyKey
	FROM FIL_Only_Reqd_Transactions
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimNumber ORDER BY NULL) = 1
),
LKP_Most_Recent_DocType_For_Claim1 AS (
	SELECT
	DOCUMENT_TYPE,
	CLAIM_ID
	FROM (
		select a.MAX_SOURCE_SEQ_NUM as SOURCE_SEQ_NUM,
			maxforclaim.DOCUMENT_TYPE as DOCUMENT_TYPE,
			RTRIM(a.CLAIM_ID) as CLAIM_ID 
		from (select ac.CLAIM_ID, 
				MAX(ac.SOURCE_SEQ_NUM) as MAX_SOURCE_SEQ_NUM
			from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract ac with (nolock)
			where ac.DATA_SOURCE = 'SRL'
			group by ac.CLAIM_ID) a
		join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract maxforclaim with (nolock) 
			on a.MAX_SOURCE_SEQ_NUM = maxforclaim.SOURCE_SEQ_NUM
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CLAIM_ID ORDER BY DOCUMENT_TYPE) = 1
),
FIL_Claim_Not_In_Sapiens AS (
	SELECT
	LKP_Most_Recent_DocType_For_Claim1.DOCUMENT_TYPE AS MostRecentDocumentType, 
	AGG_OneRowPerClaim.ClaimNumber, 
	AGG_OneRowPerClaim.PolicyKey
	FROM AGG_OneRowPerClaim
	LEFT JOIN LKP_Most_Recent_DocType_For_Claim1
	ON LKP_Most_Recent_DocType_For_Claim1.CLAIM_ID = AGG_OneRowPerClaim.ClaimNumber
	WHERE ISNULL(MostRecentDocumentType) OR MostRecentDocumentType='G'
),
LKP_SapiensReinsuranceClaimRestate_Exists AS (
	SELECT
	SapiensReinsuranceClaimRestateId,
	ClaimNumber
	FROM (
		SELECT 
			SapiensReinsuranceClaimRestateId,
			ClaimNumber
		FROM SapiensReinsuranceClaimRestate
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY ClaimNumber ORDER BY SapiensReinsuranceClaimRestateId) = 1
),
FIL_Claim_Not_In_SapiensReinsuranceClaimRestate AS (
	SELECT
	FIL_Claim_Not_In_Sapiens.ClaimNumber AS claim_num, 
	FIL_Claim_Not_In_Sapiens.PolicyKey AS curr_pol_key, 
	FIL_Claim_Not_In_Sapiens.MostRecentDocumentType, 
	LKP_SapiensReinsuranceClaimRestate_Exists.SapiensReinsuranceClaimRestateId
	FROM FIL_Claim_Not_In_Sapiens
	LEFT JOIN LKP_SapiensReinsuranceClaimRestate_Exists
	ON LKP_SapiensReinsuranceClaimRestate_Exists.ClaimNumber = FIL_Claim_Not_In_Sapiens.ClaimNumber
	WHERE ISNULL(SapiensReinsuranceClaimRestateId)
),
AGG_OneRowPerPolicy AS (
	SELECT
	curr_pol_key AS PolicyKey
	FROM FIL_Claim_Not_In_SapiensReinsuranceClaimRestate
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY NULL) = 1
),
LKP_PolicyKey_sent_to_Sapiens AS (
	SELECT
	SourceSeqNum,
	PolicyKey,
	DocumentType
	FROM (
		SELECT 
		a.maxssn as SourceSeqNum, 
		b.document_type as DocumentType,
		rtrim(a.policy_no) as PolicyKey
		from
		(select max(SOURCE_SEQ_NUM) as maxssn, 
		POLICY_NO 
		from 
		@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract
		WHERE DATA_SOURCE = 'SRP'
		group by
		POLICY_NO)a
		inner join
		@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.ArchSapiensReinsuranceHeaderExtract b
		on a.POLICY_NO = b.POLICY_NO and a.maxssn = b.SOURCE_SEQ_NUM
		WHERE b.DATA_SOURCE = 'SRP' 
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey ORDER BY SourceSeqNum DESC) = 1
),
FIL_PolicyNotSentToSapiens AS (
	SELECT
	AGG_OneRowPerPolicy.PolicyKey, 
	LKP_PolicyKey_sent_to_Sapiens.DocumentType
	FROM AGG_OneRowPerPolicy
	LEFT JOIN LKP_PolicyKey_sent_to_Sapiens
	ON LKP_PolicyKey_sent_to_Sapiens.PolicyKey = AGG_OneRowPerPolicy.PolicyKey
	WHERE ISNULL(DocumentType) 
OR DocumentType='N'
),
EXP_PreTarget AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	sysdate AS CreatedDate,
	sysdate AS ModifiedDate,
	PolicyKey,
	DocumentType AS DOCUMENT_TYPE,
	-- *INF*: IIF(DOCUMENT_TYPE='N' OR ISNULL(DOCUMENT_TYPE),'N','Y')
	-- 
	IFF(DOCUMENT_TYPE = 'N' OR DOCUMENT_TYPE IS NULL, 'N', 'Y') AS v_PolicySentFlag,
	-- *INF*: IIF(v_PolicySentFlag = 'Y' ,
	-- '1',
	-- '0')
	IFF(v_PolicySentFlag = 'Y', '1', '0') AS v_NegateFlag,
	v_NegateFlag AS o_NegateFlag,
	@{pipeline().parameters.SELECTION_START_TS} AS NegateDate,
	-- *INF*: IIF(ISNULL(:LKP.LKP_Get_Max_Sapiens_SourceSequenceNumber(1)),
	--  0,
	--  :LKP.LKP_Get_Max_Sapiens_SourceSequenceNumber(1))
	IFF(
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num IS NULL, 0,
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num
	) AS v_SourceSequenceNumber,
	v_count + 1 AS v_count,
	-- *INF*: IIF(v_NegateFlag = '1',v_SourceSequenceNumber + v_count,NULL)
	IFF(v_NegateFlag = '1', v_SourceSequenceNumber + v_count, NULL) AS SourceSequenceNumber
	FROM FIL_PolicyNotSentToSapiens
	LEFT JOIN LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1
	ON LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.ID = 1

),
SapiensReinsurancePolicyRestate AS (
	INSERT INTO SapiensReinsurancePolicyRestate
	(AuditId, CreatedDate, ModifiedDate, PolicyKey, NegateFlag, NegateDate, SourceSequenceNumber)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYKEY, 
	o_NegateFlag AS NEGATEFLAG, 
	NEGATEDATE, 
	SOURCESEQUENCENUMBER
	FROM EXP_PreTarget
),
EXP_Set_SSN_NegateFlag AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	@{pipeline().parameters.SELECTION_START_TS} AS o_CurrentTimestamp,
	-- *INF*: IIF(ISNULL(:LKP.LKP_Get_Max_Sapiens_SourceSequenceNumber(1)),
	--  0,
	--  :LKP.LKP_Get_Max_Sapiens_SourceSequenceNumber(1))
	IFF(
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num IS NULL, 0,
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num
	) AS v_lkp_Source_Seq_Num,
	v_count + 1 AS v_count,
	v_lkp_Source_Seq_Num + v_count AS o_Source_Seq_Num,
	claim_num AS ClaimNumber,
	curr_pol_key,
	'' AS DefaultChar,
	0 AS DefaultNum,
	MostRecentDocumentType AS i_DOCUMENT_TYPE,
	-- *INF*: IIF(ISNULL(i_DOCUMENT_TYPE),'0',
	--     IIF(i_DOCUMENT_TYPE='G',
	-- '0',
	-- '1')
	-- )
	IFF(i_DOCUMENT_TYPE IS NULL, '0', IFF(
	        i_DOCUMENT_TYPE = 'G', '0', '1'
	    )) AS NegateFlag
	FROM FIL_Claim_Not_In_SapiensReinsuranceClaimRestate
	LEFT JOIN LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1
	ON LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.ID = 1

),
SapiensReinsuranceClaimRestate AS (
	INSERT INTO SapiensReinsuranceClaimRestate
	(AuditId, CreatedDate, ModifiedDate, ClaimNumber, PreviousCatastropheCode, CurrentCatastropheCode, NegateDate, SourceSequenceNumber, TransactionNumber, PreviousClaimRelationshipId, CurrentClaimRelationshipId, PreviousPolicyKey, CurrentPolicyKey, NegateFlag)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CurrentTimestamp AS CREATEDDATE, 
	o_CurrentTimestamp AS MODIFIEDDATE, 
	CLAIMNUMBER, 
	DefaultChar AS PREVIOUSCATASTROPHECODE, 
	DefaultChar AS CURRENTCATASTROPHECODE, 
	o_CurrentTimestamp AS NEGATEDATE, 
	o_Source_Seq_Num AS SOURCESEQUENCENUMBER, 
	o_Source_Seq_Num AS TRANSACTIONNUMBER, 
	DefaultNum AS PREVIOUSCLAIMRELATIONSHIPID, 
	DefaultNum AS CURRENTCLAIMRELATIONSHIPID, 
	curr_pol_key AS PREVIOUSPOLICYKEY, 
	curr_pol_key AS CURRENTPOLICYKEY, 
	NEGATEFLAG
	FROM EXP_Set_SSN_NegateFlag
),