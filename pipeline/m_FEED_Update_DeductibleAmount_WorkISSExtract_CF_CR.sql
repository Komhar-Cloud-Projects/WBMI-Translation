WITH
SQ_IDO_Data AS (
	SELECT
		WP.PolicyNumber
		,WP.PolicyVersionFormatted
		,DT.HistoryID
		,DT.Type
		,ISNULL(DT.TransactionDate, DT.CreatedDate) TransactionDate
		,DS.Purpose
		,c.CoverageId
		,C.ID
		,a.Type Deductible_Type
		,CASE
			WHEN ISNUMERIC(a.Value) = 1 THEN CONVERT(MONEY, a.Value)
			ELSE 0
		END AS Value
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Deductible a
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Builder b
		ON a.ObjectId = b.CF_BuilderId
			AND a.SessionId = b.SessionId
			AND a.ObjectName = 'DC_CF_Builder'
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage c
		ON c.SessionId = b.SessionId
			AND b.CF_RiskId = c.ObjectId
			AND c.ObjectName = 'DC_CF_Risk'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT
		ON A.SessionId = DT.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP
		ON A.SessionId = WP.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS
		ON A.SessionId = DS.SessionId
	------ RFC 126190
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Risk DR
		ON DR.CF_RiskId = b.CF_RiskID
			AND DR.RiskType NOT IN ('ALS', 'BIEE', 'EXTRA', 'EETOOLS')
	------
	WHERE a.Type IN ('Standard', 'WindHail', 'DeductibleMaster', 'DeductibleWindHailMaster')
	AND ISNULL(a.Value, '0') NOT IN ('0', 'NA')
	
	@{pipeline().parameters.WHERE_CLAUSE_IDO}
	
	
	UNION
	
	SELECT
		WP.PolicyNumber
		,WP.PolicyVersionFormatted
		,DT.HistoryID
		,DT.Type
		,ISNULL(DT.TransactionDate, DT.CreatedDate) TransactionDate
		,DS.Purpose
		,d.CoverageId
		,d.ID
		,a.Type Deductible_Type
		,CASE
			WHEN ISNUMERIC(a.Value) = 1 THEN CONVERT(MONEY, a.Value)
			ELSE 0
		END AS Value
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Deductible a
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Building b
		ON a.ObjectId = b.CF_BuildingId
			AND a.SessionId = b.SessionId
			AND a.ObjectName = 'DC_CF_Building'
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Risk c
		ON c.SessionId = b.SessionId
			AND c.CF_BuildingId = b.CF_BuildingId
			AND c.RiskType NOT IN ('ALS', 'BIEE', 'EXTRA', 'EETOOLS')    ---------- RFC 126190
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage d
		ON c.SessionId = d.SessionId
			AND c.CF_RiskId = d.ObjectId
			AND d.ObjectName = 'DC_CF_Risk'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT
		ON A.SessionId = DT.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP
		ON A.SessionId = WP.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS
		ON A.SessionId = DS.SessionId
	WHERE a.Type IN ('Standard', 'WindHail', 'DeductibleMaster', 'DeductibleWindHailMaster')
	AND ISNULL(a.Value, '0') NOT IN ('0', 'NA')
	@{pipeline().parameters.WHERE_CLAUSE_IDO}
	
	UNION
	
	SELECT
		WP.PolicyNumber
		,WP.PolicyVersionFormatted
		,DT.HistoryID
		,DT.Type
		,ISNULL(DT.TransactionDate, DT.CreatedDate) TransactionDate
		,DS.Purpose
		,c.CoverageId
		,C.ID
		,a.Type Deductible_Type
		,CASE
			WHEN ISNUMERIC(a.Value) = 1 THEN CONVERT(MONEY, a.Value)
			ELSE 0
		END AS Value
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Deductible a
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Property b
		ON a.ObjectId = b.CF_PropertyId
			AND a.SessionId = b.SessionId
			AND a.ObjectName = 'DC_CF_Property'
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage c
		ON c.SessionId = b.SessionId
			AND b.CF_RiskId = c.ObjectId
			AND c.ObjectName = 'DC_CF_Risk'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT
		ON A.SessionId = DT.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP
		ON A.SessionId = WP.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS
		ON A.SessionId = DS.SessionId
	------ RFC 126190
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Risk DR
		ON DR.CF_RiskId = b.CF_RiskID
			AND DR.RiskType NOT IN ('ALS', 'BIEE', 'EXTRA', 'EETOOLS')
	------
	WHERE a.Type IN ('Standard', 'WindHail', 'DeductibleMaster', 'DeductibleWindHailMaster')
	AND ISNULL(a.Value, '0') NOT IN ('0', 'NA')
	@{pipeline().parameters.WHERE_CLAUSE_IDO}
	
	UNION
	
	SELECT
		WP.PolicyNumber
		,WP.PolicyVersionFormatted
		,DT.HistoryID
		,DT.Type
		,ISNULL(DT.TransactionDate, DT.CreatedDate) TransactionDate
		,DS.Purpose
		,c.CoverageId
		,C.ID
		,a.Type Deductible_Type
		,CASE
			WHEN ISNUMERIC(a.Value) = 1 THEN CONVERT(MONEY, a.Value)
			ELSE 0
		END AS Value
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Deductible a
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_RatingGroup b
		ON a.ObjectId = b.CF_RatingGroupId
			AND a.SessionId = b.SessionId
			AND a.ObjectName = 'DC_CF_RatingGroup'
	INNER HASH JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage c
		ON c.SessionId = b.SessionId
			AND b.CF_RiskId = c.ObjectId
			AND c.ObjectName = 'DC_CF_Risk'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT
		ON A.SessionId = DT.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP
		ON A.SessionId = WP.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS
		ON A.SessionId = DS.SessionId
	------ RFC 126190
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Risk DR
		ON DR.CF_RiskId = b.CF_RiskID
			AND DR.RiskType NOT IN ('ALS', 'BIEE', 'EXTRA', 'EETOOLS')
	------
	WHERE a.Type IN ('Standard', 'WindHail', 'DeductibleMaster', 'DeductibleWindHailMaster')
	AND ISNULL(a.Value, '0') NOT IN ('0', 'NA')
	@{pipeline().parameters.WHERE_CLAUSE_IDO}
	
	UNION
	
	SELECT
		WP.PolicyNumber
		,WP.PolicyVersionFormatted
		,DT.HistoryID
		,DT.Type
		,ISNULL(DT.TransactionDate, DT.CreatedDate) TransactionDate
		,DS.Purpose
		,A.CoverageId
		,A.ID
		,C.Type Deductible_Type
		,CASE
			WHEN ISNUMERIC(C.Value) = 1 THEN CONVERT(MONEY, C.Value)
			ELSE 0
		END AS Value
	FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Coverage A
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CF_Building B
		ON A.SessionId = B.SessionId
			AND A.ObjectId = B.LineId
			AND A.ObjectName = 'DC_Line'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Deductible C
		ON B.SessionId = C.SessionId
			AND B.CF_BuildingId = C.ObjectId
			AND C.ObjectName = 'DC_CF_Building'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction DT
		ON A.SessionId = DT.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP
		ON A.SessionId = WP.SessionId
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session DS
		ON A.SessionId = DS.SessionId
	WHERE C.Type IN ('Standard', 'WindHail', 'DeductibleMaster', 'DeductibleWindHailMaster')
	AND ISNULL(C.Value, '0') NOT IN ('0', 'NA')
	@{pipeline().parameters.WHERE_CLAUSE_IDO}
),
EXP_IDO_Data_Collect AS (
	SELECT
	PolicyNumber,
	PolicyVersionFormatted,
	HistoryID,
	Type,
	TransactionDate,
	Purpose,
	CoverageId,
	Id,
	Deductible_Type,
	Value,
	-- *INF*: IIF(Deductible_Type='Standard',Value,'0')
	IFF(Deductible_Type = 'Standard', Value, '0') AS Standard_Deductible,
	-- *INF*: IIF(Deductible_Type='WindHail',Value,'0')
	IFF(Deductible_Type = 'WindHail', Value, '0') AS WindHail_Deductible,
	-- *INF*: IIF(Deductible_Type='DeductibleMaster',Value,'0')
	IFF(Deductible_Type = 'DeductibleMaster', Value, '0') AS DeductibleMaster_Deductible,
	-- *INF*: IIF(Deductible_Type='DeductibleWindHailMaster',Value,'0')
	IFF(Deductible_Type = 'DeductibleWindHailMaster', Value, '0') AS DeductibleWindHailMaster_Deductible
	FROM SQ_IDO_Data
),
SRT_IDO_Data AS (
	SELECT
	PolicyNumber, 
	PolicyVersionFormatted, 
	HistoryID, 
	Type, 
	TransactionDate, 
	Purpose, 
	CoverageId, 
	Id, 
	Standard_Deductible, 
	WindHail_Deductible, 
	DeductibleMaster_Deductible, 
	DeductibleWindHailMaster_Deductible
	FROM EXP_IDO_Data_Collect
	ORDER BY PolicyNumber ASC, PolicyVersionFormatted ASC, HistoryID ASC, Type ASC, TransactionDate ASC, Purpose ASC, CoverageId ASC, Id ASC
),
AGG_Remove_Duplicates AS (
	SELECT
	PolicyNumber,
	PolicyVersionFormatted,
	HistoryID,
	Type,
	TransactionDate,
	Purpose,
	CoverageId,
	Id,
	Standard_Deductible,
	-- *INF*: Max(Standard_Deductible)
	Max(Standard_Deductible) AS o_Standard_Deductible,
	WindHail_Deductible,
	-- *INF*: Max(WindHail_Deductible)
	Max(WindHail_Deductible) AS o_WindHail_Deductible,
	DeductibleMaster_Deductible,
	-- *INF*: Max(DeductibleMaster_Deductible)
	Max(DeductibleMaster_Deductible) AS o_DeductibleMaster_Deductible,
	DeductibleWindHailMaster_Deductible,
	-- *INF*: Max(DeductibleWindHailMaster_Deductible)
	Max(DeductibleWindHailMaster_Deductible) AS o_DeductibleWindHailMaster_Deductible
	FROM SRT_IDO_Data
	GROUP BY PolicyNumber, PolicyVersionFormatted, HistoryID, Type, TransactionDate, Purpose, CoverageId, Id
),
EXP_IDO_Data AS (
	SELECT
	PolicyNumber,
	PolicyVersionFormatted,
	PolicyNumber || PolicyVersionFormatted AS Pol_key,
	HistoryID,
	Type,
	TransactionDate,
	Purpose,
	CoverageId,
	Id,
	o_Standard_Deductible AS Standard_Deductible,
	o_WindHail_Deductible AS WindHail_Deductible,
	o_DeductibleMaster_Deductible AS DeductibleMaster_Deductible,
	o_DeductibleWindHailMaster_Deductible AS DeductibleWindHailMaster_Deductible
	FROM AGG_Remove_Duplicates
),
LKP_RatingCoverageAKID AS (
	SELECT
	RatingCoverageAKID,
	IN_pol_key,
	IN_CoverageGUID,
	IN_TransactionDate,
	Pol_key,
	CoverageGUID,
	EffectiveDate
	FROM (
		select distinct RC.RatingCoverageAKID as RatingCoverageAKID,
		P.Pol_key as Pol_key,
		RC.CoverageGUID as CoverageGUID,
		RC.EffectiveDate as EffectiveDate
		from @{pipeline().parameters.SOURCE_TABLE_OWNER_V2}.policy P
		inner join @{pipeline().parameters.DATAFEEDMART_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.ISSCommercialPropertyExtract I
		on P.pol_key=I.Policykey
		and P.source_sys_id='DCT'
		and P.crrnt_snpsht_flag=1
		inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.RatingCoverage RC
		on P.pol_ak_id=cast(substring(RC.RatingCoverageKey,1,charindex('~',RC.RatingCoverageKey,1)-1) as bigint)
		and RC.PolicyCoverageAKID<>-1
		where I.EDWPremiumMasterCalculationPKId<>-1
		and I.SublineCode not in ('070','090','170','190','930','931')
		and I.DeductibleAmount in ('0000000','N/A')
		and I.AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY Pol_key,CoverageGUID,EffectiveDate ORDER BY RatingCoverageAKID) = 1
),
EXP_IDO_PTHashKey AS (
	SELECT
	EXP_IDO_Data.Pol_key,
	EXP_IDO_Data.HistoryID,
	EXP_IDO_Data.Type,
	EXP_IDO_Data.TransactionDate,
	EXP_IDO_Data.Purpose,
	EXP_IDO_Data.CoverageId,
	EXP_IDO_Data.Id,
	LKP_RatingCoverageAKID.RatingCoverageAKID,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'Onset' || 'Onset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'Onset' || 'Onset') AS Onset_HashKey,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'Offset' || 'Onset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'Offset' || 'Onset') AS Offset_HashKey,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'N/A' || 'Onset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'N/A' || 'Onset') AS NA_HashKey,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'Onset' || 'Offset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'Onset' || 'Offset') AS DepOnset_HashKey,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'Offset' || 'Offset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'Offset' || 'Offset') AS DepOffset_HashKey,
	-- *INF*: MD5(RatingCoverageAKID|| Id||TO_CHAR(TransactionDate)|| 'N/A' || 'Offset')
	MD5(RatingCoverageAKID || Id || TO_CHAR(TransactionDate) || 'N/A' || 'Offset') AS DepNA_HashKey,
	EXP_IDO_Data.Standard_Deductible,
	EXP_IDO_Data.WindHail_Deductible,
	EXP_IDO_Data.DeductibleMaster_Deductible,
	EXP_IDO_Data.DeductibleWindHailMaster_Deductible
	FROM EXP_IDO_Data
	LEFT JOIN LKP_RatingCoverageAKID
	ON LKP_RatingCoverageAKID.Pol_key = EXP_IDO_Data.Pol_key AND LKP_RatingCoverageAKID.CoverageGUID = EXP_IDO_Data.Id AND LKP_RatingCoverageAKID.EffectiveDate = EXP_IDO_Data.TransactionDate
),
SQ_PremiumTransaction AS (
	Select A.ISSCommercialPropertyExtractId,
	A.SublineCode,
	C.PremiumTransactionID,
	C.PremiumTransactionHashKey,
	C.PremiumTransactionAKID
	from @{pipeline().parameters.DATAFEEDMART_DATABASE}.@{pipeline().parameters.SOURCE_TABLE_OWNER}.ISSCommercialPropertyExtract A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumMasterCalculation B
	on A.EDWPremiumMasterCalculationPKId=B.PremiumMasterCalculationID
	and B.SourceSystemID='DCT'
	INNER JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.PremiumTransaction C
	on B.PremiumTransactionAKID=C.PremiumTransactionAKID
	and C.SourceSystemID='DCT'
	where A.EDWPremiumMasterCalculationPKId<>-1
	and A.SublineCode not in ('070','090','170','190','930','931')
	and A.DeductibleAmount in ('0000000','N/A')
	and A.AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE_CLAUSE_EDW}
),
JNR_DEP_NA AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.DepNA_HashKey, 
	EXP_IDO_PTHashKey.Standard_Deductible, 
	EXP_IDO_PTHashKey.WindHail_Deductible, 
	SQ_PremiumTransaction.ISSCommercialPropertyExtractId, 
	SQ_PremiumTransaction.SublineCode, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID, 
	EXP_IDO_PTHashKey.DeductibleMaster_Deductible, 
	EXP_IDO_PTHashKey.DeductibleWindHailMaster_Deductible
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.DepNA_HashKey
),
JNR_DEP_OFFSET AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.DepOffset_HashKey, 
	EXP_IDO_PTHashKey.Standard_Deductible, 
	EXP_IDO_PTHashKey.WindHail_Deductible, 
	SQ_PremiumTransaction.ISSCommercialPropertyExtractId, 
	SQ_PremiumTransaction.SublineCode, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID, 
	EXP_IDO_PTHashKey.DeductibleMaster_Deductible, 
	EXP_IDO_PTHashKey.DeductibleWindHailMaster_Deductible
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.DepOffset_HashKey
),
JNR_DEP_ONSET AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.DepOnset_HashKey, 
	EXP_IDO_PTHashKey.Standard_Deductible, 
	EXP_IDO_PTHashKey.WindHail_Deductible, 
	SQ_PremiumTransaction.ISSCommercialPropertyExtractId, 
	SQ_PremiumTransaction.SublineCode, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID, 
	EXP_IDO_PTHashKey.DeductibleMaster_Deductible, 
	EXP_IDO_PTHashKey.DeductibleWindHailMaster_Deductible
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.DepOnset_HashKey
),
JNR_NA AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.NA_HashKey, 
	EXP_IDO_PTHashKey.Standard_Deductible, 
	EXP_IDO_PTHashKey.WindHail_Deductible, 
	SQ_PremiumTransaction.ISSCommercialPropertyExtractId, 
	SQ_PremiumTransaction.SublineCode, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID, 
	EXP_IDO_PTHashKey.DeductibleMaster_Deductible, 
	EXP_IDO_PTHashKey.DeductibleWindHailMaster_Deductible
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.NA_HashKey
),
JNR_OFFSET AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.Offset_HashKey, 
	EXP_IDO_PTHashKey.Standard_Deductible, 
	EXP_IDO_PTHashKey.WindHail_Deductible, 
	SQ_PremiumTransaction.ISSCommercialPropertyExtractId, 
	SQ_PremiumTransaction.SublineCode, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID, 
	EXP_IDO_PTHashKey.DeductibleMaster_Deductible, 
	EXP_IDO_PTHashKey.DeductibleWindHailMaster_Deductible
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.Offset_HashKey
),
JNR_ONSET AS (SELECT
	EXP_IDO_PTHashKey.CoverageId, 
	EXP_IDO_PTHashKey.Onset_HashKey, 
	EXP_IDO_PTHashKey.Standard_Deductible, 
	EXP_IDO_PTHashKey.WindHail_Deductible, 
	SQ_PremiumTransaction.ISSCommercialPropertyExtractId, 
	SQ_PremiumTransaction.SublineCode, 
	SQ_PremiumTransaction.PremiumTransactionID, 
	SQ_PremiumTransaction.PremiumTransactionHashKey, 
	SQ_PremiumTransaction.PremiumTransactionAKID, 
	EXP_IDO_PTHashKey.DeductibleMaster_Deductible, 
	EXP_IDO_PTHashKey.DeductibleWindHailMaster_Deductible
	FROM EXP_IDO_PTHashKey
	INNER JOIN SQ_PremiumTransaction
	ON SQ_PremiumTransaction.PremiumTransactionHashKey = EXP_IDO_PTHashKey.Onset_HashKey
),
UN_Merge_All_Data_Flows AS (
	SELECT ISSCommercialPropertyExtractId, SublineCode, Standard_Deductible, WindHail_Deductible, DeductibleMaster_Deductible, DeductibleWindHailMaster_Deductible
	FROM JNR_ONSET
	UNION
	SELECT ISSCommercialPropertyExtractId, SublineCode, Standard_Deductible, WindHail_Deductible, DeductibleMaster_Deductible, DeductibleWindHailMaster_Deductible
	FROM JNR_OFFSET
	UNION
	SELECT ISSCommercialPropertyExtractId, SublineCode, Standard_Deductible, WindHail_Deductible, DeductibleMaster_Deductible, DeductibleWindHailMaster_Deductible
	FROM JNR_NA
	UNION
	SELECT ISSCommercialPropertyExtractId, SublineCode, Standard_Deductible, WindHail_Deductible, DeductibleMaster_Deductible, DeductibleWindHailMaster_Deductible
	FROM JNR_DEP_ONSET
	UNION
	SELECT ISSCommercialPropertyExtractId, SublineCode, Standard_Deductible, WindHail_Deductible, DeductibleMaster_Deductible, DeductibleWindHailMaster_Deductible
	FROM JNR_DEP_OFFSET
	UNION
	SELECT ISSCommercialPropertyExtractId, SublineCode, Standard_Deductible, WindHail_Deductible, DeductibleMaster_Deductible, DeductibleWindHailMaster_Deductible
	FROM JNR_DEP_NA
),
EXP_DeductibleAmount AS (
	SELECT
	ISSCommercialPropertyExtractId,
	SublineCode,
	Standard_Deductible,
	WindHail_Deductible,
	DeductibleMaster_Deductible,
	DeductibleWindHailMaster_Deductible,
	-- *INF*: IIF(IN(SublineCode,'020','027','120','010','015','016','017','018','029','035','045','055','110'),Standard_Deductible,'0')
	IFF(
	    SublineCode IN ('020','027','120','010','015','016','017','018','029','035','045','055','110'),
	    Standard_Deductible,
	    '0'
	) AS v_Standard_DeductibleAmount,
	-- *INF*: IIF(IN(SublineCode,'020','027','120'),WindHail_Deductible,'0')
	IFF(SublineCode IN ('020','027','120'), WindHail_Deductible, '0') AS v_WindHail_DeductibleAmount,
	-- *INF*: IIF(IN(SublineCode,'020','027','120','010','015','016','017','018','029','035','045','055','110'),DeductibleMaster_Deductible,'0')
	IFF(
	    SublineCode IN ('020','027','120','010','015','016','017','018','029','035','045','055','110'),
	    DeductibleMaster_Deductible,
	    '0'
	) AS v_DeductibleMaster_Deductible,
	-- *INF*: IIF(IN(SublineCode,'020','027','120'),DeductibleWindHailMaster_Deductible,'0')
	IFF(SublineCode IN ('020','027','120'), DeductibleWindHailMaster_Deductible, '0') AS v_DeductibleWindHailMaster_Deductible,
	-- *INF*: IIF(ISNULL(v_Standard_DeductibleAmount) or v_Standard_DeductibleAmount='0' or length(rtrim(ltrim(v_Standard_DeductibleAmount)))=0,v_DeductibleMaster_Deductible,v_Standard_DeductibleAmount)
	IFF(
	    v_Standard_DeductibleAmount IS NULL
	    or v_Standard_DeductibleAmount = '0'
	    or length(rtrim(ltrim(v_Standard_DeductibleAmount))) = 0,
	    v_DeductibleMaster_Deductible,
	    v_Standard_DeductibleAmount
	) AS v_Standard,
	-- *INF*: IIF(ISNULL(v_WindHail_DeductibleAmount) or v_WindHail_DeductibleAmount='0' or length(rtrim(ltrim(v_WindHail_DeductibleAmount)))=0,v_DeductibleWindHailMaster_Deductible,v_WindHail_DeductibleAmount)
	IFF(
	    v_WindHail_DeductibleAmount IS NULL
	    or v_WindHail_DeductibleAmount = '0'
	    or length(rtrim(ltrim(v_WindHail_DeductibleAmount))) = 0,
	    v_DeductibleWindHailMaster_Deductible,
	    v_WindHail_DeductibleAmount
	) AS v_WindHail,
	-- *INF*: LPAD(IIF(ISNULL(v_WindHail) or v_WindHail='0' or length(rtrim(ltrim(v_WindHail)))=0,v_Standard,v_WindHail), 7, '0')
	LPAD(
	    IFF(
	        v_WindHail IS NULL or v_WindHail = '0' or length(rtrim(ltrim(v_WindHail))) = 0,
	        v_Standard,
	        v_WindHail
	    ), 7, '0') AS o_DeductibleAmount
	FROM UN_Merge_All_Data_Flows
),
FIL_Zero_Deductible AS (
	SELECT
	ISSCommercialPropertyExtractId, 
	o_DeductibleAmount AS DeductibleAmount
	FROM EXP_DeductibleAmount
	WHERE NOT(IN(DeductibleAmount,'0000000','N/A'))
),
UPD_DeductibleAmount AS (
	SELECT
	ISSCommercialPropertyExtractId, 
	DeductibleAmount
	FROM FIL_Zero_Deductible
),
ISSCommercialPropertyExtract1 AS (
	MERGE INTO ISSCommercialPropertyExtract AS T
	USING UPD_DeductibleAmount AS S
	ON T.ISSCommercialPropertyExtractId = S.ISSCommercialPropertyExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.DeductibleAmount = S.DeductibleAmount
),
SQ_ISSCommercialPropertyExtract_UpdateDeductibleAmount AS (
	select 
	A.ISSCommercialPropertyExtractId as ISSCommercialPropertyExtractId, 
	A.PolicyKey as PolicyKey,
	A.LocationNumber as LocationNumber, 
	A.BuildingNumber as BuildingNumber, 
	A.AuditId as AuditId,
	A.PremiumMasterCoverageEffectiveDate as PremiumMasterCoverageEffectiveDate,
	A.SublineCode as SublineCode,
	A.PremiumMasterRunDate as PremiumMasterRunDate,
	B.DeductibleAmount as DeductibleAmount
	
	FROM
	 (
	SELECT
	ISS.PolicyKey,
	RC.CoverageType, 
	RC.RiskType, 
	ISS.PremiumMasterRunDate,
	RC.SublineCode, 
	RC.RatingCoverageAKID, 
	EDWPremiumMasterCalculationPKId, 
	ISSCommercialPropertyExtractId, 
	ISS.CoverageCode, 
	ISS.LocationNumber, 
	ISS.BuildingNumber,
	ISS.AuditId,
	ISS.DeductibleAmount,
	PremiumMasterCoverageEffectiveDate
	FROM 
	@{pipeline().parameters.DATAFEEDMART_DATABASE}.dbo.ISSCommercialPropertyExtract ISS with (nolock)
	INNER JOIN PremiumMasterCalculation PMC with (nolock) on ISS.EDWPremiumMasterCalculationPKId=PMC.PremiumMasterCalculationID
	INNER JOIN RatingCoverage RC with (nolock) on PMC.RatingCoverageAKId = RC.RatingCoverageAKID AND PMC.RatingCoverageEffectiveDate=RC.EffectiveDate
	where 
	ISS.DeductibleAmount IN ('N/A','0000000') AND
	ISS.TypeBureauCode='CF' AND
	RC.SublineCode NOT IN ('070','090','930','N/A')  AND
	(
	(RC.RiskType='ALS' and (RC.CoverageType='ALS' or RC.CoverageType like '%agreed%')) OR
	(RC.RiskType='BIEE' and (RC.CoverageType LIKE 'businessincome%' or RC.CoverageType LIKE 'agreed%')) OR
	(RC.CoverageType LIKE 'BIEE%') OR
	(RC.CoverageType LIKE 'Extra%') OR
	(RC.CoverageType LIKE 'time%') OR
	(RC.RiskType='TIME' AND (RC.CoverageType LIKE 'businessincome%' OR RC.CoverageType LIKE 'agreed%'))
	)
	AND ISS.AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	) A
	
	OUTER APPLY
	
	(
	select top 1
	ISS.PolicyKey,
	RC.CoverageType, 
	RC.RiskType, 
	ISS.PremiumMasterRunDate,
	RC.SublineCode, 
	RC.RatingCoverageAKID, 
	EDWPremiumMasterCalculationPKId, 
	ISSCommercialPropertyExtractId, 
	ISS.CoverageCode, 
	ISS.LocationNumber, 
	ISS.BuildingNumber,
	case when RC.CoverageType='BLDG' then 1
	when RC.CoverageType='BusinessPersProperty' then 2 else 3 end as OrderSequence,
	ISS.AuditId,
	PremiumMasterCoverageEffectiveDate,
	ISS.DeductibleAmount
	FROM 
	@{pipeline().parameters.DATAFEEDMART_DATABASE}.dbo.ISSCommercialPropertyExtract ISS with (nolock)
	INNER JOIN PremiumMasterCalculation PMC with (nolock) on ISS.EDWPremiumMasterCalculationPKId=PMC.PremiumMasterCalculationID
	INNER JOIN RatingCoverage RC with (nolock) on PMC.RatingCoverageAKId = RC.RatingCoverageAKID and PMC.RatingCoverageEffectiveDate=RC.EffectiveDate
	where 
	ISS.DeductibleAmount NOT IN ('N/A','0000000') AND
	ISS.TypeBureauCode='CF' AND
	ISS.PremiumMasterRunDate =A.PremiumMasterRunDate AND
	RC.SublineCode NOT IN ('070','090','930','N/A')  AND
	(
	RC.CoverageType IN ('BLDG','BusinessPersProperty')
	) AND 
	ISS.PolicyKey=A.PolicyKey AND 
	ISS.LocationNumber=A.LocationNumber AND 
	(ISS.BuildingNumber=A.BuildingNumber OR A.BuildingNumber='000') AND 
	ISS.AuditId=A.AuditId AND 
	PMC.PremiumMasterCoverageEffectiveDate=A.PremiumMasterCoverageEffectiveDate AND 
	ISS.SublineCode=A.SublineCode
	order by 12
	) B
),
EXP_UpdateDedAmount_Input AS (
	SELECT
	ISSCommercialPropertyExtractId,
	PolicyKey,
	LocationNumber,
	BuildingNumber,
	AuditId,
	PremiumMasterCoverageEffectiveDate,
	SublineCode,
	PremiumMasterRunDate,
	DeductibleAmount
	FROM SQ_ISSCommercialPropertyExtract_UpdateDeductibleAmount
),
RTR_UpdateDedAmount AS (
	SELECT
	ISSCommercialPropertyExtractId,
	PolicyKey,
	AuditId,
	PremiumMasterCoverageEffectiveDate,
	PremiumMasterRunDate,
	DeductibleAmount
	FROM EXP_UpdateDedAmount_Input
),
RTR_UpdateDedAmount_Update AS (SELECT * FROM RTR_UpdateDedAmount WHERE NOT ISNULL(DeductibleAmount)),
RTR_UpdateDedAmount_AdditionalLookup AS (SELECT * FROM RTR_UpdateDedAmount WHERE ISNULL(DeductibleAmount)),
LKP_IISCommericalPropertyExtract_DedAmount AS (
	SELECT
	AuditId,
	PremiumMasterCoverageEffectiveDate,
	PolicyKey,
	PremiumMasterRunDate,
	DeductibleAmount,
	in_ISSCommercialPropertyExtractId,
	in_PolicyKey,
	in_AuditId,
	in_PremiumMasterCoverageEffectiveDate,
	in_PremiumMasterRunDate
	FROM (
		SELECT DISTINCT
		ISS.AuditId as AuditId,
		PMC.PremiumMasterCoverageEffectiveDate as PremiumMasterCoverageEffectiveDate,
		ISS.PolicyKey as PolicyKey,
		ISS.PremiumMasterRunDate as PremiumMasterRunDate,
		ISS.DeductibleAmount as DeductibleAmount
		FROM 
		@{pipeline().parameters.DATAFEEDMART_DATABASE}.dbo.ISSCommercialPropertyExtract ISS with (nolock)
		INNER JOIN
		PremiumMasterCalculation PMC with (nolock) on ISS.EDWPremiumMasterCalculationPKId=PMC.PremiumMasterCalculationID
		INNER JOIN 
		RatingCoverage RC with (nolock) on PMC.RatingCoverageAKId = RC.RatingCoverageAKID and PMC.RatingCoverageEffectiveDate=RC.EffectiveDate 
		WHERE
		-- just build cache for this audit id
		 ISS.AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
		 and ISS.SublineCode='035'
		 and ISS.LocationNumber='0001' and ISS.BuildingNumber='000'
		 and ISS.CoverageCode='03'
		 and ISS.DeductibleAmount not in ('N/A','0000000') 
		 and ISS.TypeBureauCode='CF'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PolicyKey,AuditId,PremiumMasterCoverageEffectiveDate,PremiumMasterRunDate ORDER BY AuditId) = 1
),
FIL_RemoveNull_UpdateDedAmount AS (
	SELECT
	DeductibleAmount, 
	in_ISSCommercialPropertyExtractId
	FROM LKP_IISCommericalPropertyExtract_DedAmount
	WHERE NOT ISNULL(DeductibleAmount)
),
Union_UpdateDedAmount AS (
	SELECT ISSCommercialPropertyExtractId, DeductibleAmount
	FROM RTR_UpdateDedAmount_Update
	UNION
	SELECT in_ISSCommercialPropertyExtractId AS ISSCommercialPropertyExtractId, DeductibleAmount
	FROM FIL_RemoveNull_UpdateDedAmount
),
UPDTRANS AS (
	SELECT
	ISSCommercialPropertyExtractId, 
	DeductibleAmount
	FROM Union_UpdateDedAmount
),
ISSCommercialPropertyExtract_UPDATE_DedAmount AS (
	MERGE INTO ISSCommercialPropertyExtract AS T
	USING UPDTRANS AS S
	ON T.ISSCommercialPropertyExtractId = S.ISSCommercialPropertyExtractId
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.DeductibleAmount = S.DeductibleAmount
),