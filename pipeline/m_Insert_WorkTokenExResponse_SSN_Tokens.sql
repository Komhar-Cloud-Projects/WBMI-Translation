WITH
SQ_Token_Response_File AS (

-- TODO Manual --

),
EXP_Input AS (
	SELECT
	SYSDATE AS Created_Date,
	SYSDATE AS Modified_Date,
	SSN_ID,
	SSN_Tokens
	FROM SQ_Token_Response_File
),
WorkTokenExResponse AS (
	TRUNCATE TABLE worktokenexresponse;
	INSERT INTO worktokenexresponse
	(CreatedDate, ModifiedDate, SSNId, SSNToken)
	SELECT 
	Created_Date AS CREATEDDATE, 
	Modified_Date AS MODIFIEDDATE, 
	SSN_ID AS SSNID, 
	SSN_Tokens AS SSNTOKEN
	FROM EXP_Input
),