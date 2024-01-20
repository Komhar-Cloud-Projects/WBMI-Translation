WITH
SQ_Claim_Transaction AS (
	select rc.claimant_cov_det_reserve_calculation_id, rc.claimant_cov_det_reserve_calculation_ak_id, 
		rc.claimant_cov_det_ak_id, rc.financial_type_code, rc.reserve_date, rc.reserve_date_type
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claimant_coverage_detail_reserve_calculation rc with (nolock) 
	join (select distinct ct.claimant_cov_det_ak_id, ct.financial_type_code
		from @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction ct with (nolock) 
		where ct.trans_date > '@{pipeline().parameters.SELECTION_START_TS}'
		and ct.crrnt_snpsht_flag = 1) t
		on t.claimant_cov_det_ak_id = rc.claimant_cov_det_ak_id 
	    		and t.financial_type_code = rc.financial_type_code
	where rc.crrnt_snpsht_flag = 1
),
EXP_Input AS (
	SELECT
	claimant_cov_det_reserve_calculation_id,
	claimant_cov_det_reserve_calculation_ak_id,
	claimant_cov_det_ak_id,
	financial_type_code,
	reserve_date,
	reserve_date_type
	FROM SQ_Claim_Transaction
),
LKP_First_Payment_Date AS (
	SELECT
	trans_date,
	claimant_cov_det_ak_id,
	financial_type_code
	FROM (
		SELECT ct.trans_date as trans_date, ct.claimant_cov_det_ak_id as claimant_cov_det_ak_id, ct.financial_type_code as financial_type_code 
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.claim_transaction ct with (nolock)
		where ct.trans_code in ('20','21','22','23','24','28','29','42','43')
		and ct.trans_offset_onset_ind in ('N','N/A')
		and ct.crrnt_snpsht_flag = 1
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY claimant_cov_det_ak_id,financial_type_code ORDER BY trans_date) = 1
),
EXP_Collect_Reserve_Information AS (
	SELECT
	EXP_Input.claimant_cov_det_reserve_calculation_id AS ClaimantCoverageDetailReserveCalcId,
	EXP_Input.claimant_cov_det_reserve_calculation_ak_id AS ClaimantCoverageDetailReserveCalcAkId,
	EXP_Input.claimant_cov_det_ak_id AS ClaimantCoverageDetailAkId,
	EXP_Input.financial_type_code AS FinancialTypeCode,
	EXP_Input.reserve_date AS ReserveDate,
	EXP_Input.reserve_date_type AS ReserveDateType,
	LKP_First_Payment_Date.trans_date AS FirstPaymentDate,
	-- *INF*: IIF(ISNULL(FirstPaymentDate),
	-- TO_DATE('1/1/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	-- FirstPaymentDate)
	IFF(
	    FirstPaymentDate IS NULL, TO_TIMESTAMP('1/1/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),
	    FirstPaymentDate
	) AS FirstPaymentDateFromSource,
	-- *INF*: DECODE(TRUE,
	-- ReserveDateType='2OPEN',
	-- ReserveDate,
	-- TO_DATE('1/1/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
	DECODE(
	    TRUE,
	    ReserveDateType = '2OPEN', ReserveDate,
	    TO_TIMESTAMP('1/1/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	) AS ReserveOpenDateFromSource,
	-- *INF*: DECODE(TRUE,
	-- ReserveDateType='3CLOSED',
	-- ReserveDate,
	-- TO_DATE('1/1/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
	DECODE(
	    TRUE,
	    ReserveDateType = '3CLOSED', ReserveDate,
	    TO_TIMESTAMP('1/1/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	) AS ReserveCloseDateFromSource,
	-- *INF*: DECODE(TRUE,
	-- ReserveDateType='4REOPEN',
	-- ReserveDate,
	-- TO_DATE('1/1/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
	DECODE(
	    TRUE,
	    ReserveDateType = '4REOPEN', ReserveDate,
	    TO_TIMESTAMP('1/1/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	) AS ReserveReopenDateFromSource,
	-- *INF*: DECODE(TRUE,
	-- ReserveDateType='5CLOSEDAFTERREOPEN',
	-- ReserveDate,
	-- TO_DATE('1/1/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'))
	DECODE(
	    TRUE,
	    ReserveDateType = '5CLOSEDAFTERREOPEN', ReserveDate,
	    TO_TIMESTAMP('1/1/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS')
	) AS ReserveCloseAfterReopenDateFromSource,
	SYSDATE AS created_modified_date,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId
	FROM EXP_Input
	LEFT JOIN LKP_First_Payment_Date
	ON LKP_First_Payment_Date.claimant_cov_det_ak_id = EXP_Input.claimant_cov_det_ak_id AND LKP_First_Payment_Date.financial_type_code = EXP_Input.financial_type_code
),
mplt_ClaimReserveDim AS (WITH
	INPUT AS (
		
	),
	EXP_Get_Values AS (
		SELECT
		claimant_coverage_detail_ak_id AS ClaimantCoverageDetailAkId,
		financial_type_code AS in_FinancialTypeCode,
		-- *INF*: RTRIM(in_FinancialTypeCode)
		RTRIM(in_FinancialTypeCode) AS out_FinancialTypeCode
		FROM INPUT
	),
	LKP_Existing_Reserve AS (
		SELECT
		ClaimReserveDimId,
		ReserveOpenDate,
		ReserveCloseDate,
		ReserveReopenDate,
		ReserveCloseAfterReopenDate,
		FirstPaymentDate,
		EDWClaimantCoverageDetailAKId,
		FinancialTypeCode
		FROM (
			SELECT 
				ClaimReserveDimId,
				ReserveOpenDate,
				ReserveCloseDate,
				ReserveReopenDate,
				ReserveCloseAfterReopenDate,
				FirstPaymentDate,
				EDWClaimantCoverageDetailAKId,
				FinancialTypeCode
			FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.ClaimReserveDim
		)
		QUALIFY ROW_NUMBER() OVER (PARTITION BY EDWClaimantCoverageDetailAKId,FinancialTypeCode ORDER BY ClaimReserveDimId DESC) = 1
	),
	OUTPUT AS (
		SELECT
		ClaimReserveDimId, 
		ReserveOpenDate, 
		ReserveCloseDate, 
		ReserveReopenDate, 
		ReserveCloseAfterReopenDate, 
		FirstPaymentDate
		FROM LKP_Existing_Reserve
	),
),
RTR_Insert_or_Update AS (
	SELECT
	mplt_ClaimReserveDim.ClaimReserveDimId,
	EXP_Collect_Reserve_Information.ClaimantCoverageDetailReserveCalcId,
	EXP_Collect_Reserve_Information.ClaimantCoverageDetailReserveCalcAkId,
	EXP_Collect_Reserve_Information.ClaimantCoverageDetailAkId,
	EXP_Collect_Reserve_Information.FinancialTypeCode,
	EXP_Collect_Reserve_Information.FirstPaymentDateFromSource,
	EXP_Collect_Reserve_Information.ReserveOpenDateFromSource,
	EXP_Collect_Reserve_Information.ReserveCloseDateFromSource,
	EXP_Collect_Reserve_Information.ReserveReopenDateFromSource,
	EXP_Collect_Reserve_Information.ReserveCloseAfterReopenDateFromSource,
	EXP_Collect_Reserve_Information.created_modified_date,
	EXP_Collect_Reserve_Information.AuditId,
	mplt_ClaimReserveDim.ReserveOpenDate AS LKP_ReserveOpenDate,
	mplt_ClaimReserveDim.ReserveCloseDate AS LKP_ReserveCloseDate,
	mplt_ClaimReserveDim.ReserveReopenDate AS LKP_ReserveReopenDate,
	mplt_ClaimReserveDim.ReserveCloseAfterReopenDate AS LKP_ReserveCloseAfterReopenDate,
	mplt_ClaimReserveDim.FirstPaymentDate AS LKP_FirstPaymentDate
	FROM EXP_Collect_Reserve_Information
	 -- Manually join with mplt_ClaimReserveDim
),
RTR_Insert_or_Update_Insert AS (SELECT * FROM RTR_Insert_or_Update WHERE ISNULL(ClaimReserveDimId)),
RTR_Insert_or_Update_Update AS (SELECT * FROM RTR_Insert_or_Update WHERE (NOT ISNULL(ClaimReserveDimId)) AND
((ReserveOpenDateFromSource<>LKP_ReserveOpenDate)
	OR (ReserveCloseDateFromSource<>LKP_ReserveCloseDate)
	OR (ReserveReopenDateFromSource<>LKP_ReserveReopenDate)
	OR (ReserveCloseAfterReopenDateFromSource<>LKP_ReserveCloseAfterReopenDate) 
	OR (FirstPaymentDateFromSource<>LKP_FirstPaymentDate))),
UPD_existing_records AS (
	SELECT
	ClaimReserveDimId, 
	ClaimantCoverageDetailReserveCalcId, 
	ClaimantCoverageDetailReserveCalcAkId, 
	ClaimantCoverageDetailAkId, 
	FinancialTypeCode, 
	FirstPaymentDateFromSource AS FirstPaymentDate, 
	ReserveOpenDateFromSource AS ReserveOpenDate, 
	ReserveCloseDateFromSource AS ReserveCloseDate, 
	ReserveReopenDateFromSource AS ReserveReopenDate, 
	ReserveCloseAfterReopenDateFromSource AS ReserveCloseAfterReopenDate, 
	created_modified_date, 
	AuditId
	FROM RTR_Insert_or_Update_Update
),
ClaimReserveDim_Update AS (
	MERGE INTO ClaimReserveDim AS T
	USING UPD_existing_records AS S
	ON T.ClaimReserveDimId = S.ClaimReserveDimId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.AuditId = S.AuditId, T.ModifiedDate = S.created_modified_date, T.EDWClaimantCoverageDetailReserveCalculationPKId = S.ClaimantCoverageDetailReserveCalcId, T.EDWClaimantCoverageDetReserveCalculationAKId = S.ClaimantCoverageDetailReserveCalcAkId, T.EDWClaimantCoverageDetailAKId = S.ClaimantCoverageDetailAkId, T.FinancialTypeCode = S.FinancialTypeCode, T.ReserveOpenDate = S.ReserveOpenDate, T.ReserveCloseDate = S.ReserveCloseDate, T.ReserveReopenDate = S.ReserveReopenDate, T.ReserveCloseAfterReopenDate = S.ReserveCloseAfterReopenDate, T.FirstPaymentDate = S.FirstPaymentDate
),
ClaimReserveDim_Insert AS (
	INSERT INTO ClaimReserveDim
	(AuditId, CreatedDate, ModifiedDate, EDWClaimantCoverageDetailReserveCalculationPKId, EDWClaimantCoverageDetReserveCalculationAKId, EDWClaimantCoverageDetailAKId, FinancialTypeCode, ReserveOpenDate, ReserveCloseDate, ReserveReopenDate, ReserveCloseAfterReopenDate, FirstPaymentDate)
	SELECT 
	AUDITID, 
	created_modified_date AS CREATEDDATE, 
	created_modified_date AS MODIFIEDDATE, 
	ClaimantCoverageDetailReserveCalcId AS EDWCLAIMANTCOVERAGEDETAILRESERVECALCULATIONPKID, 
	ClaimantCoverageDetailReserveCalcAkId AS EDWCLAIMANTCOVERAGEDETRESERVECALCULATIONAKID, 
	ClaimantCoverageDetailAkId AS EDWCLAIMANTCOVERAGEDETAILAKID, 
	FINANCIALTYPECODE, 
	ReserveOpenDateFromSource AS RESERVEOPENDATE, 
	ReserveCloseDateFromSource AS RESERVECLOSEDATE, 
	ReserveReopenDateFromSource AS RESERVEREOPENDATE, 
	ReserveCloseAfterReopenDateFromSource AS RESERVECLOSEAFTERREOPENDATE, 
	FirstPaymentDateFromSource AS FIRSTPAYMENTDATE
	FROM RTR_Insert_or_Update_Insert
),