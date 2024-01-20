WITH
SQ_EDW_Claim_Tables AS (
	SELECT co.claim_occurrence_key as claim_occurrence_key, 
	      cp.claim_party_key as claim_party_key,
		RTRIM(co.s3p_claim_num) as s3p_claim_num,
		cp.claim_party_full_name as claim_party_full_name,
		cp.claim_party_birthdate as claim_party_birthdate,
		RTRIM(cpo.claim_party_role_code) as claim_party_role_code,
		co.claim_occurrence_ak_id as claim_occurrence_ak_id,
		cp.claim_party_ak_id as claim_party_ak_id,
		ROW_NUMBER() OVER (PARTITION BY co.claim_occurrence_ak_id, cp.claim_party_ak_id ORDER BY cpo.claim_party_role_code) as RoleNumberForCPO,
	cp.claim_party_state as Party_State,
		cp.claim_party_city as Party_City,
		cp.claim_party_addr as Party_Street,
		cp.claim_party_zip as Party_Zip
	FROM dbo.claim_party_occurrence cpo with (nolock) 
	INNER JOIN dbo.claim_occurrence co with (nolock) ON co.claim_occurrence_ak_id = cpo.claim_occurrence_ak_id AND co.crrnt_snpsht_flag = 1
	and cpo.crrnt_snpsht_flag = 1
	INNER JOIN dbo.claim_party cp with (nolock) ON cpo.claim_party_ak_id = cp.claim_party_ak_id AND cp.crrnt_snpsht_flag = 1
	AND co.source_sys_id = 'EXCEED'
	@{pipeline().parameters.WHERE_CLAUSE}
	ORDER BY co.claim_occurrence_key,cp.claim_party_full_name
),
EXP_Evaluate AS (
	SELECT
	claim_occurrence_key,
	claim_party_key,
	s3p_claim_num,
	claim_party_full_name,
	claim_party_birthdate,
	claim_party_role_code,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	RoleNumberForCPO,
	-- *INF*: DECODE(TRUE,
	-- claim_party_birthdate = TO_DATE('12/31/9999','MM/DD/YYYY'),'N',
	-- claim_party_birthdate = TO_DATE('12/31/2100','MM/DD/YYYY'),'N',
	-- DATE_DIFF(SYSDATE, claim_party_birthdate,'YYYY') < 18,'Y',
	-- 'N')
	-- 
	-- 
	-- --IIF(DATE_DIFF(SYSDATE, claim_party_birthdate,'YYYY') < 18, 'Y','N')
	DECODE(
	    TRUE,
	    claim_party_birthdate = TO_TIMESTAMP('12/31/9999', 'MM/DD/YYYY'), 'N',
	    claim_party_birthdate = TO_TIMESTAMP('12/31/2100', 'MM/DD/YYYY'), 'N',
	    DATEDIFF(YEAR,CURRENT_TIMESTAMP,claim_party_birthdate) < 18, 'Y',
	    'N'
	) AS MinorFlag,
	-- *INF*: IIF(v_prev_row_claim_occurrence_ak_id = claim_occurrence_ak_id  AND v_prev_row_claim_party_ak_id = claim_party_ak_id,
	-- v_prev_row_role_values  ||  ', '  ||  claim_party_role_code, claim_party_role_code)
	IFF(
	    v_prev_row_claim_occurrence_ak_id = claim_occurrence_ak_id
	    and v_prev_row_claim_party_ak_id = claim_party_ak_id,
	    v_prev_row_role_values || ', ' || claim_party_role_code,
	    claim_party_role_code
	) AS v_role_values,
	v_role_values AS Role_values,
	claim_occurrence_ak_id AS v_prev_row_claim_occurrence_ak_id,
	claim_party_ak_id AS v_prev_row_claim_party_ak_id,
	v_role_values AS v_prev_row_role_values,
	claim_party_state,
	claim_party_city,
	claim_party_addr,
	claim_party_zip
	FROM SQ_EDW_Claim_Tables
),
AGG_Data AS (
	SELECT
	claim_occurrence_key,
	s3p_claim_num,
	claim_party_full_name,
	claim_party_key,
	claim_party_birthdate,
	claim_occurrence_ak_id,
	claim_party_ak_id,
	RoleNumberForCPO,
	MinorFlag,
	Role_values,
	-- *INF*: LAST(Role_values)
	LAST(Role_values) AS Out_Role_values,
	claim_party_state,
	claim_party_city,
	claim_party_addr,
	claim_party_zip
	FROM EXP_Evaluate
	GROUP BY claim_occurrence_ak_id, claim_party_ak_id
),
Claim_Party_File AS (
	INSERT INTO FF_Claim_Party_File
	(Claim_Number, Party_Id, Claim_Party_Full_Name, Minor_Flag, Rolevalues, Party_State, Party_City, Party_Street, Party_Zip)
	SELECT 
	s3p_claim_num AS CLAIM_NUMBER, 
	claim_party_key AS PARTY_ID, 
	claim_party_full_name AS CLAIM_PARTY_FULL_NAME, 
	MinorFlag AS MINOR_FLAG, 
	Out_Role_values AS ROLEVALUES, 
	claim_party_state AS PARTY_STATE, 
	claim_party_city AS PARTY_CITY, 
	claim_party_addr AS PARTY_STREET, 
	claim_party_zip AS PARTY_ZIP
	FROM AGG_Data
),