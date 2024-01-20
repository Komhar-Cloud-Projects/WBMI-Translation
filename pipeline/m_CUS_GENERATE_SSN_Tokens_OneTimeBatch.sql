WITH
Exceed_Claim_Customer AS (
	SELECT DISTINCT Exceed_Claim_Customer.Tax_Ssn_Id 
	FROM
	 Exceed_Claim_Customer
),
EXP_Exceed_Claim_Customer AS (
	SELECT
	Tax_Ssn_Id
	FROM Exceed_Claim_Customer
),
PMS_ADJUSTER_MASTER_STAGE AS (
	SELECT Distinct PMS_ADJUSTER_MASTER_STAGE.ADNM_TAXID_SSN 
	FROM
	 PMS_ADJUSTER_MASTER_STAGE
),
EXP_PMS_ADJUSTER_MASTER_STAGE AS (
	SELECT
	ADNM_TAXID_SSN
	FROM PMS_ADJUSTER_MASTER_STAGE
),
Pms_Claim_Customer AS (
	SELECT Distinct Pms_Claim_Customer.Tax_Ssn_Id 
	FROM
	 Pms_Claim_Customer
),
EXP_Pms_Claim_Customer AS (
	SELECT
	Tax_Ssn_Id
	FROM Pms_Claim_Customer
),
pif_42gj_stage AS (
	SELECT DISTINCT pif_42gj_stage.ipfc4j_id_number 
	FROM
	 pif_42gj_stage
),
EXP_pif_42gj_stage AS (
	SELECT
	ipfc4j_id_number
	FROM pif_42gj_stage
),
Union AS (
	SELECT Tax_Ssn_Id
	FROM EXP_Exceed_Claim_Customer
	UNION
	SELECT ADNM_TAXID_SSN AS Tax_Ssn_Id
	FROM EXP_PMS_ADJUSTER_MASTER_STAGE
	UNION
	SELECT Tax_Ssn_Id
	FROM EXP_Pms_Claim_Customer
	UNION
	SELECT ipfc4j_id_number AS Tax_Ssn_Id
	FROM EXP_pif_42gj_stage
),
EXP_Value AS (
	SELECT
	Tax_Ssn_Id AS i_taxid_ssn,
	-- *INF*: IIF(REG_MATCH(i_taxid_ssn,'[\da-zA-Z]+'),'FALSE',(LTRIM(RTRIM(i_taxid_ssn))))
	-- 
	-- 
	-- 
	-- 
	-- --(LTRIM(RTRIM(i_taxid_ssn)))
	IFF(REGEXP_LIKE(i_taxid_ssn, '[\da-zA-Z]+'), 'FALSE', (LTRIM(RTRIM(i_taxid_ssn)))) AS o_taxid_ssn
	FROM Union
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
		-- *INF*: IIF( SUBSTR(V_ssn_fein_taxid,3,1)='-' OR  (SUBSTR(V_ssn_fein_taxid,1,3)='000' AND (LENGTH(V_ssn_fein_taxid)=9 ) ) OR SUBSTR(V_ssn_fein_taxid,2,1)='-'  OR   (TO_INTEGER(SUBSTR(V_ssn_fein_taxid,1,3))>=750 AND (LENGTH(V_ssn_fein_taxid)=9 ) )OR ISNULL(V_ssn_fein_taxid) OR (V_ssn_fein_taxid='N/A')   OR REG_MATCH(V_ssn_fein_taxid,'[*]*') OR(REG_MATCH(V_ssn_fein_taxid,'[\da-zA-Z]+') AND (LENGTH(V_ssn_fein_taxid)=11 OR LENGTH(V_ssn_fein_taxid)=10) )
		--  OR ((SUBSTR(V_ssn_fein_taxid,4,1)='-')  AND  (LENGTH(V_ssn_fein_taxid) != 11 )) OR (LENGTH(V_ssn_fein_taxid)<=6 ) OR  (LENGTH(V_ssn_fein_taxid)>11 ) ,'FEIN','NONFEIN')
		-- 
		-- 
		-- 
		-- 
		-- 
		IFF(
		    SUBSTR(V_ssn_fein_taxid, 3, 1) = '-'
		    or (SUBSTR(V_ssn_fein_taxid, 1, 3) = '000'
		    and (LENGTH(V_ssn_fein_taxid) = 9))
		    or SUBSTR(V_ssn_fein_taxid, 2, 1) = '-'
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
		    or (LENGTH(V_ssn_fein_taxid) <= 6)
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
AGG_Remove_Duplicates AS (
	SELECT
	OUT_id AS o_ID,
	OUT_valid_ssn AS taxid_ssn
	FROM mplt_SSN_Check
	QUALIFY ROW_NUMBER() OVER (PARTITION BY o_ID, taxid_ssn ORDER BY NULL) = 1
),
Claims_OneTime_Conversion_Batch_File AS (
	INSERT INTO TEST_File
	(ID, SSN_TOKENS)
	SELECT 
	o_ID AS ID, 
	taxid_ssn AS SSN_TOKENS
	FROM AGG_Remove_Duplicates
),