WITH
SQ_SapiensReinsuranceClaimNegate AS (
	SELECT
		SapiensReinsuranceClaimRestateId,
		AuditId,
		CreatedDate,
		ModifiedDate,
		ClaimNumber,
		PreviousLossDate,
		CurrentLossDate,
		PreviousCatastropheCode,
		CurrentCatastropheCode,
		NegateDate,
		SourceSequenceNumber,
		TransactionNumber,
		PreviousClaimRelationshipId,
		CurrentClaimRelationshipId,
		PreviousPolicyKey,
		CurrentPolicyKey,
		NegateFlag
	FROM SapiensReinsuranceClaimRestate
),
EXP_Collect AS (
	SELECT
	SapiensReinsuranceClaimRestateId,
	AuditId,
	CreatedDate,
	ModifiedDate,
	ClaimNumber,
	PreviousLossDate,
	CurrentLossDate,
	PreviousCatastropheCode,
	CurrentCatastropheCode,
	NegateDate,
	SourceSequenceNumber,
	TransactionNumber,
	PreviousClaimRelationshipId,
	CurrentClaimRelationshipId,
	PreviousPolicyKey,
	CurrentPolicyKey,
	NegateFlag
	FROM SQ_SapiensReinsuranceClaimNegate
),
ArchSapiensReinsuranceClaimRestate AS (
	INSERT INTO ArchSapiensReinsuranceClaimRestate
	(AuditId, CreatedDate, ModifiedDate, SapiensReinsuranceClaimRestateId, ClaimNumber, PreviousLossDate, CurrentLossDate, PreviousCatastropheCode, CurrentCatastropheCode, NegateDate, SourceSequenceNumber, TransactionNumber, PreviousClaimRelationshipId, CurrentClaimRelationshipId, PreviousPolicyKey, CurrentPolicyKey, NegateFlag)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SAPIENSREINSURANCECLAIMRESTATEID, 
	CLAIMNUMBER, 
	PREVIOUSLOSSDATE, 
	CURRENTLOSSDATE, 
	PREVIOUSCATASTROPHECODE, 
	CURRENTCATASTROPHECODE, 
	NEGATEDATE, 
	SOURCESEQUENCENUMBER, 
	TRANSACTIONNUMBER, 
	PREVIOUSCLAIMRELATIONSHIPID, 
	CURRENTCLAIMRELATIONSHIPID, 
	PREVIOUSPOLICYKEY, 
	CURRENTPOLICYKEY, 
	NEGATEFLAG
	FROM EXP_Collect
),