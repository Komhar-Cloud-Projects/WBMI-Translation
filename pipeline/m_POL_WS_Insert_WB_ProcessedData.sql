WITH
SQ_WorkDCTInBalancePolicy AS (
	Select distinct A.WorkDCTInBalancePolicyId,A.HistoryID,A.Purpose 
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTInBalancePolicy A 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTPolicy B
	on A.SessionID=B.SessionId 
	where A.ExtractDate>'@{pipeline().parameters.SELECTION_END_TS}'
	and A.AccountingDate is NOT NULL
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_MetaData AS (
	SELECT
	PK_Cntr+1 AS PK_Cntr,
	-- *INF*: CEIL(PK_Cntr / @{pipeline().parameters.WEB_BATCH_SIZE})
	CEIL(PK_Cntr / @{pipeline().parameters.WEB_BATCH_SIZE}) AS XPK_n3_Envelope,
	SYSDATE AS tns_CreateDate,
	'EDW' AS tns_ProcessName,
	WorkDCTInBalancePolicyId,
	HistoryID,
	Purpose
	FROM SQ_WorkDCTInBalancePolicy
),
LoadProcessedData AS (-- LoadProcessedData

	##############################################

	# TODO: Place holder for Custom transformation

	##############################################
),
WB_ProccessedData_DummyTarget AS (
	INSERT INTO WB_ProccessedData_DummyTarget
	(DummyKey)
	SELECT 
	XPK_n3_Envelope0 AS DUMMYKEY
	FROM LoadProcessedData
),