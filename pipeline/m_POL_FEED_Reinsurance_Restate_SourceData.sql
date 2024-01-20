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
SQ_SapiensPolicyRestate AS (

-- TODO Manual --

),
SQ_SapiensPolicyRestateClaimsMade AS (

-- TODO Manual --

),
JNR_UsrList_ClaimsMadeList AS (SELECT
	SQ_SapiensPolicyRestate.Policy, 
	SQ_SapiensPolicyRestate.NegateFlag, 
	SQ_SapiensPolicyRestate.UserName, 
	SQ_SapiensPolicyRestate.DateTime, 
	SQ_SapiensPolicyRestateClaimsMade.Policy AS Policy_CM, 
	SQ_SapiensPolicyRestateClaimsMade.NegateFlag AS NegateFlag_CM, 
	SQ_SapiensPolicyRestateClaimsMade.UserName AS UserName_CM, 
	SQ_SapiensPolicyRestateClaimsMade.DateTime AS DateTime_CM
	FROM SQ_SapiensPolicyRestate
	FULL OUTER JOIN SQ_SapiensPolicyRestateClaimsMade
	ON SQ_SapiensPolicyRestateClaimsMade.Policy = SQ_SapiensPolicyRestate.Policy
),
EXPTRANS AS (
	SELECT
	Policy,
	NegateFlag,
	UserName,
	DateTime,
	Policy_CM,
	NegateFlag_CM,
	UserName_CM,
	DateTime_CM,
	-- *INF*: IIF(ISNULL(Policy),Ltrim(rtrim(Policy_CM)),Ltrim(rtrim(Policy)))
	IFF(Policy IS NULL, Ltrim(rtrim(Policy_CM)), Ltrim(rtrim(Policy))) AS O_Policy,
	-- *INF*: IIF(ISNULL(NegateFlag_CM),NegateFlag,NegateFlag_CM)
	IFF(NegateFlag_CM IS NULL, NegateFlag, NegateFlag_CM) AS O_NegateFlag,
	-- *INF*: IIF(ISNULL(UserName_CM),UserName,UserName_CM)
	IFF(UserName_CM IS NULL, UserName, UserName_CM) AS O_UserName,
	-- *INF*: IIF(ISNULL(DateTime_CM),DateTime,DateTime_CM)
	IFF(DateTime_CM IS NULL, DateTime, DateTime_CM) AS O_DateTime
	FROM JNR_UsrList_ClaimsMadeList
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
EXP_PreTarget AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	sysdate AS CreatedDate,
	sysdate AS ModifiedDate,
	EXPTRANS.O_Policy AS PolicyKey,
	EXPTRANS.O_NegateFlag AS i_NegateFlag,
	LKP_PolicyKey_sent_to_Sapiens.DocumentType AS DOCUMENT_TYPE,
	-- *INF*: IIF(DOCUMENT_TYPE='N' OR ISNULL(DOCUMENT_TYPE),'N','Y')
	-- 
	IFF(DOCUMENT_TYPE = 'N' OR DOCUMENT_TYPE IS NULL, 'N', 'Y') AS v_PolicySentFlag,
	-- *INF*: IIF(v_PolicySentFlag = 'Y' ,
	-- IIF(i_NegateFlag = 'Y','1','0'),
	-- '0')
	IFF(v_PolicySentFlag = 'Y', IFF(
	        i_NegateFlag = 'Y', '1', '0'
	    ), '0') AS v_NegateFlag,
	v_NegateFlag AS o_NegateFlag,
	@{pipeline().parameters.SELECTION_START_TS} AS o_NegateDate,
	-- *INF*: IIF(ISNULL(:LKP.LKP_Get_Max_Sapiens_SourceSequenceNumber(1)),
	--  0,
	--  :LKP.LKP_Get_Max_Sapiens_SourceSequenceNumber(1))
	IFF(
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num IS NULL, 0,
	    LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.Source_Seq_Num
	) AS v_SourceSequenceNumber,
	v_count + 1 AS v_count,
	-- *INF*: IIF(v_NegateFlag = '1',v_SourceSequenceNumber + v_count,NULL)
	IFF(v_NegateFlag = '1', v_SourceSequenceNumber + v_count, NULL) AS SourceSequenceNumber,
	LKP_PolicyKey_sent_to_Sapiens.SourceSeqNum AS SOURCE_SEQ_NUM,
	LKP_PolicyKey_sent_to_Sapiens.PolicyKey AS POLICY_NO
	FROM EXPTRANS
	LEFT JOIN LKP_PolicyKey_sent_to_Sapiens
	ON LKP_PolicyKey_sent_to_Sapiens.PolicyKey = EXPTRANS.O_Policy
	LEFT JOIN LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1
	ON LKP_GET_MAX_SAPIENS_SOURCESEQUENCENUMBER_1.ID = 1

),
SapiensReinsurancePolicyRestate AS (
	TRUNCATE TABLE SapiensReinsurancePolicyRestate;
	INSERT INTO SapiensReinsurancePolicyRestate
	(AuditId, CreatedDate, ModifiedDate, PolicyKey, NegateFlag, NegateDate, SourceSequenceNumber)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYKEY, 
	o_NegateFlag AS NEGATEFLAG, 
	o_NegateDate AS NEGATEDATE, 
	SOURCESEQUENCENUMBER
	FROM EXP_PreTarget
),
SQ_SapiensReinsuranceClaimsRestate AS (
	select distinct 
	    P.Pol_Key as PolicyKey,
	    CASE WHEN cod.claim_num = 'N/A' THEN RTRIM(cod.claim_occurrence_key) ELSE RTRIM(cod.claim_num) END as ClaimNumber
	FROM @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_loss_transaction_fact cltf with (nolock) 
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.policy_dim P with (nolock) ON cltf.pol_dim_id = P.pol_dim_id
		INNER JOIN @{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_occurrence_dim cod with (nolock) ON cltf.claim_occurrence_dim_id = cod.claim_occurrence_dim_id
	      INNER JOIN @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.SapiensReinsurancePolicyRestate srps with (nolock) ON P.pol_key = srps.PolicyKey
),
EXP_Consolidated AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	SYSDATE AS CreatedDate,
	SYSDATE AS ModifiedDate,
	ClaimKey AS ClaimNumber,
	'' AS Defaultchar,
	0 AS DefaultNum,
	PolicyKey,
	'0' AS NegateFlag
	FROM SQ_SapiensReinsuranceClaimsRestate
),
SapiensReinsuranceClaimRestate AS (
	TRUNCATE TABLE SapiensReinsuranceClaimRestate;
	INSERT INTO SapiensReinsuranceClaimRestate
	(AuditId, CreatedDate, ModifiedDate, ClaimNumber, PreviousCatastropheCode, CurrentCatastropheCode, PreviousClaimRelationshipId, CurrentClaimRelationshipId, CurrentPolicyKey, NegateFlag)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	CLAIMNUMBER, 
	Defaultchar AS PREVIOUSCATASTROPHECODE, 
	Defaultchar AS CURRENTCATASTROPHECODE, 
	DefaultNum AS PREVIOUSCLAIMRELATIONSHIPID, 
	DefaultNum AS CURRENTCLAIMRELATIONSHIPID, 
	PolicyKey AS CURRENTPOLICYKEY, 
	NEGATEFLAG
	FROM EXP_Consolidated
),