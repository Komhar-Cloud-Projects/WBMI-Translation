WITH
SQ_SapiensReinsurancePolicyRestate AS (
	SELECT
		SapiensReinsurancePolicyRestateId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		PolicyKey,
		NegateFlag,
		NegateDate,
		SourceSequenceNumber
	FROM SapiensReinsurancePolicyRestate
),
EXP_Passthrough AS (
	SELECT
	SapiensReinsurancePolicyRestateId,
	AuditId,
	CreatedDate,
	ModifiedDate,
	PolicyKey,
	NegateFlag,
	NegateDate,
	SourceSequenceNumber
	FROM SQ_SapiensReinsurancePolicyRestate
),
ArchSapiensReinsurancePolicyRestate AS (
	INSERT INTO ArchSapiensReinsurancePolicyRestate
	(AuditId, CreatedDate, ModifiedDate, SapiensReinsurancePolicyRestateId, PolicyKey, NegateFlag, NegateDate, SourceSequenceNumber)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SAPIENSREINSURANCEPOLICYRESTATEID, 
	POLICYKEY, 
	NEGATEFLAG, 
	NEGATEDATE, 
	SOURCESEQUENCENUMBER
	FROM EXP_Passthrough
),