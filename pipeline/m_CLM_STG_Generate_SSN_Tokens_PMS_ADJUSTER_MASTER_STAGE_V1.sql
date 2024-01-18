WITH
SQ_ADJMSTR_AJNUMM53___Header AS (

-- TODO Manual --

),
EXP_Input AS (
	SELECT
	ADNM_TAXID_SSN AS IN_ADNM_TAXID_SSN,
	-- *INF*: (LTRIM(RTRIM(IN_ADNM_TAXID_SSN)))
	(LTRIM(RTRIM(IN_ADNM_TAXID_SSN))) AS OUT_ADNM_TAXID_SSN
	FROM SQ_ADJMSTR_AJNUMM53___Header
),
mplt_SSN_Check AS (WITH
	INPUT AS (
		
	),
	EXP_SSN_FEIN_TAXID AS (
		SELECT
		IN_id AS id,
		IN_ssn_fein_id AS ssn_fein_taxid,
		-- *INF*: LTRIM(RTRIM(ssn_fein_taxid))
		LTRIM(RTRIM(ssn_fein_taxid)) AS V_ssn_fein_taxid,
		-- *INF*: IIF( SUBSTR(V_ssn_fein_taxid,3,1)='-' OR SUBSTR(V_ssn_fein_taxid,2,1)='-' OR  (SUBSTR(V_ssn_fein_taxid,1,3)='000' AND (LENGTH(V_ssn_fein_taxid)=9 ) ) OR   (TO_INTEGER(SUBSTR(V_ssn_fein_taxid,1,3))>=750 AND (LENGTH(V_ssn_fein_taxid)=9 ) )OR ISNULL(V_ssn_fein_taxid) OR (V_ssn_fein_taxid='N/A') OR REG_MATCH(V_ssn_fein_taxid,'[*]*') OR(REG_MATCH(V_ssn_fein_taxid,'[\da-zA-Z]+') AND (LENGTH(V_ssn_fein_taxid)=11 OR LENGTH(V_ssn_fein_taxid)=10) )
		--  OR ((SUBSTR(V_ssn_fein_taxid,4,1)='-')  AND  (LENGTH(V_ssn_fein_taxid) != 11 )) OR (LENGTH(V_ssn_fein_taxid)<=5 ) OR  (LENGTH(V_ssn_fein_taxid)>11 ) ,'FEIN','NONFEIN')
		-- 
		--  
		-- 
		-- 
		-- 
		IFF(
		    SUBSTR(V_ssn_fein_taxid, 3, 1) = '-'
		    or SUBSTR(V_ssn_fein_taxid, 2, 1) = '-'
		    or (SUBSTR(V_ssn_fein_taxid, 1, 3) = '000'
		    and (LENGTH(V_ssn_fein_taxid) = 9))
		    or (CAST(SUBSTR(V_ssn_fein_taxid, 1, 3) AS INTEGER) >= 750
		    and (LENGTH(V_ssn_fein_taxid) = 9))
		    or V_ssn_fein_taxid IS NULL
		    or (V_ssn_fein_taxid = 'N/A')
		    or REGEXP_LIKE(V_ssn_fein_taxid, '[*]*')
		    or (REGEXP_LIKE(V_ssn_fein_taxid, '[\da-zA-Z]+')
		    and (LENGTH(V_ssn_fein_taxid) = 11
		    or LENGTH(V_ssn_fein_taxid) = 10))
		    or ((SUBSTR(V_ssn_fein_taxid, 4, 1) = '-')
		    and (LENGTH(V_ssn_fein_taxid) != 11))
		    or (LENGTH(V_ssn_fein_taxid) <= 5)
		    or (LENGTH(V_ssn_fein_taxid) > 11),
		    'FEIN',
		    'NONFEIN'
		) AS V_flag,
		V_flag AS flag,
		-- *INF*: IIF(LENGTH(V_ssn_fein_taxid)>=7 AND LENGTH(V_ssn_fein_taxid)<=8,LPAD(V_ssn_fein_taxid,9,'0'),V_ssn_fein_taxid)
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		IFF(
		    LENGTH(V_ssn_fein_taxid) >= 7 AND LENGTH(V_ssn_fein_taxid) <= 8,
		    LPAD(V_ssn_fein_taxid, 9, '0'),
		    V_ssn_fein_taxid
		) AS V_taxid,
		-- *INF*: IIF(REG_MATCH(V_taxid,'[0-9-]*') ,V_taxid,'X')
		-- 
		-- 
		-- 
		IFF(REGEXP_LIKE(V_taxid, '[0-9-]*'), V_taxid, 'X') AS V_valid_taxid,
		V_valid_taxid AS flag_TaxId,
		-- *INF*: IIF(LENGTH(V_valid_taxid)=9  AND (REG_MATCH(V_valid_taxid,'^[0-9]*$'))  ,(SUBSTR(V_valid_taxid, 1, 3) ||'-'||SUBSTR(V_valid_taxid, 4, 2)||'-'||SUBSTR(V_valid_taxid, 6, 4)) ,V_valid_taxid)
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		-- 
		IFF(
		    LENGTH(V_valid_taxid) = 9 AND (REGEXP_LIKE(V_valid_taxid, '^[0-9]*$')),
		    (SUBSTR(V_valid_taxid, 1, 3) || '-' || SUBSTR(V_valid_taxid, 4, 2) || '-' || SUBSTR(V_valid_taxid, 6, 4)),
		    V_valid_taxid
		) AS OUT_taxid
		FROM INPUT
	),
	RTR_SSN_FEIN_TAXID AS (
		SELECT
		id,
		flag,
		ssn_fein_taxid AS fein_taxid,
		OUT_taxid AS ssn,
		flag_TaxId
		FROM EXP_SSN_FEIN_TAXID
	),
	RTR_SSN_FEIN_TAXID_FEIN AS (SELECT * FROM RTR_SSN_FEIN_TAXID WHERE flag = 'FEIN'),
	RTR_SSN_FEIN_TAXID_SSN AS (SELECT * FROM RTR_SSN_FEIN_TAXID WHERE flag='NONFEIN'  AND flag_TaxId != 'X'),
	AGGTRANS AS (
		SELECT
		id AS Id,
		ssn AS SSN
		FROM RTR_SSN_FEIN_TAXID_SSN
		QUALIFY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY NULL) = 1
	),
	OUTPUT AS (
		SELECT
		Id AS OUT_id, 
		SSN AS OUT_valid_ssn
		FROM AGGTRANS
	),
),
EXP_Values AS (
	SELECT
	OUT_id AS ID,
	'Tokenize' AS Function,
	'SSN' AS Scheme,
	OUT_valid_ssn AS SSN_FEIN_TAXID,
	'BATCHCYCLEJOB' AS Requestedby,
	'Claims' AS Application,
	'PMS_ADJUSTER_MASTER' AS Caller,
	'pms_adjuster_master_stage.csv' AS File_Name,
	-- *INF*: IIF(v_SeqNumber = 0,@{pipeline().parameters.BATCHSIZE},v_SeqNumber + 1)
	IFF(v_SeqNumber = 0, @{pipeline().parameters.BATCHSIZE}, v_SeqNumber + 1) AS v_SeqNumber,
	-- *INF*: TRUNC(v_SeqNumber / @{pipeline().parameters.BATCHSIZE},0)
	TRUNC(v_SeqNumber / @{pipeline().parameters.BATCHSIZE},0) AS v_batch_number,
	v_SeqNumber AS Out_SeqNumber,
	v_batch_number AS Out_batchNumber
	FROM mplt_SSN_Check
),
AGGTRANS AS (
	SELECT
	Out_batchNumber,
	Out_SeqNumber,
	ID,
	Function,
	Scheme,
	Requestedby,
	Application,
	Caller,
	File_Name,
	SSN_FEIN_TAXID
	FROM EXP_Values
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Out_batchNumber ORDER BY NULL) = 1
),
Tokenize_WebServiceCall AS (-- Tokenize_WebServiceCall

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
FILTRANS AS (
	SELECT
	tns_ResponseData0 AS TokenResponse
	FROM Tokenize_WebServiceCall
	WHERE FALSE
),
CLM_STG_Insert_PMS_ADJUSTER_MASTER_STAGE_TOKENS_FILE_V1 AS (
	INSERT INTO CLM_STG_Insert_PMS_ADJUSTER_MASTER_STAGE_TOKENS_FILE
	(ADNM_TAXID_SSN_ID, ADNM_TAXID_SSN_TOKENS)
	SELECT 
	TokenResponse AS ADNM_TAXID_SSN_ID, 
	TokenResponse AS ADNM_TAXID_SSN_TOKENS
	FROM FILTRANS
),