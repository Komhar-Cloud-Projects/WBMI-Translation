WITH
SQ_DCTDividend AS (
	SELECT 
	DCTDividend.DCTDividendId, 
	DCTDividend.PolicyAKId,
	DCTDividend.StateCode
	FROM
	@{pipeline().parameters.SOURCE_DATABASE_NAME}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.DCTDividend 
	WHERE
	DCTDividend.DividendPaidAmount <> 0
	and PolicyAKId in (
	select PolicyAKId from DCTDividend
	where SourceSystemId = 'DCT' and DividendPaidAmount <> 0
	group by PolicyAKId
	having count(distinct StateCode) > 1)
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Passthrough AS (
	SELECT
	DCTDividendId,
	PolicyAKId,
	StateCode
	FROM SQ_DCTDividend
),
mplt_UPDATE_DCTDividend_Zeroes AS (WITH
	INPUT_DCTDividend_Zero AS (
		
	),
	EXP_passthrough AS (
		SELECT
		DCTDividendId,
		PolicyAKId,
		StateCode
		FROM INPUT_DCTDividend_Zero
	),
	LKP_Pol AS (
		SELECT
		pol_ak_id,
		state_code
		FROM (
			select 
			s.state_code as state_code,
			p.pol_ak_id as pol_ak_id
			from @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy p
			inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER_V2}.Agency a
			on p.AgencyAKID = a.AgencyAKID
			inner join sup_state s
			on s.state_abbrev = a.AssignedStateCode and s.crrnt_snpsht_flag = 1
			where p.crrnt_snpsht_flag = 1 
			and a.CurrentSnapshotFlag = 1 
			and p.source_sys_id = 'DCT'
			--
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_ak_id ORDER BY pol_ak_id DESC) = 1
	),
	EXP_Target AS (
		SELECT
		EXP_passthrough.DCTDividendId,
		EXP_passthrough.PolicyAKId,
		EXP_passthrough.StateCode,
		LKP_Pol.state_code AS lkp_State_code
		FROM EXP_passthrough
		LEFT JOIN LKP_Pol
		ON LKP_Pol.pol_ak_id = EXP_passthrough.PolicyAKId
	),
	FIL_Non_Agency_states AS (
		SELECT
		DCTDividendId, 
		PolicyAKId, 
		StateCode, 
		lkp_State_code
		FROM EXP_Target
		WHERE StateCode != lkp_State_code
	),
	OUTPUT_Update_DCTDividend_Zeroes AS (
		SELECT
		DCTDividendId, 
		PolicyAKId, 
		StateCode, 
		lkp_State_code
		FROM FIL_Non_Agency_states
	),
),
EXP_PreTarget AS (
	SELECT
	DCTDividendId1 AS DCTDividendId,
	0 AS Defaultamount
	FROM mplt_UPDATE_DCTDividend_Zeroes
),
UPD_DCTDividend AS (
	SELECT
	DCTDividendId, 
	Defaultamount
	FROM EXP_PreTarget
),
DCTDividend_Update AS (
	MERGE INTO DCTDividend AS T
	USING UPD_DCTDividend AS S
	ON T.DCTDividendId = S.DCTDividendId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.DividendPaidAmount = S.Defaultamount
),