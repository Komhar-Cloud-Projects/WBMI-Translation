WITH
SQ_SapiensClaimError AS (
	SELECT
	sce.SapiensClaimErrorId 
	FROM
	SapiensClaimError sce
	where exists
	(select 1 from  SapiensReinsuranceClaim src
					where src.ClaimTransactionPKId = sce.ClaimTransactionPKId)
),
UPDTRANS AS (
	SELECT
	SapiensClaimErrorId
	FROM SQ_SapiensClaimError
),
SapiensClaimError_Delete AS (
	DELETE FROM SapiensClaimError
	WHERE (SapiensClaimErrorId) IN (SELECT  SAPIENSCLAIMERRORID FROM UPDTRANS)
),