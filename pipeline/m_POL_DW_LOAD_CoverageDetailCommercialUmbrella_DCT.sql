WITH
SQ_DCCUUmbrella AS (
	with DCCOVERAGE as 
	(
	 SELECT sessionid,
	       lineid,
	       NULL             AS BO_Description,
	       NULL             AS BO_PolicyNumber,
	       NULL             AS CA_Description,
	       NULL             AS CA_PolicyNumber,
	       UMEL.description  AS EL_Description,
	       UMEL.policynumber AS EL_PolicyNumber,
	       NULL             AS GL_Description,
	       NULL             AS GL_PolicyNumber,
	       NULL             AS SMART_Description,
	       NULL             AS SMART_PolicyNumber,
	       NULL             AS SBOP_Description,
	       NULL             AS SBOP_PolicyNumber,
	       NULL              AS RetroDate
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.dccuumbrellaemployersliabilitystaging UMEL
	UNION ALL
	SELECT sessionid,
	       lineid,
	       NULL             AS BO_Description,
	       NULL             AS BO_PolicyNumber,
	       UMCA.description  AS CA_Description,
	       UMCA.policynumber AS CA_PolicyNumber,
	       NULL             AS EL_Description,
	       NULL             AS EL_PolicyNumber,
	       NULL             AS GL_Description,
	       NULL             AS GL_PolicyNumber,
	       NULL             AS SMART_Description,
	       NULL             AS SMART_PolicyNumber,
	       NULL             AS SBOP_Description,
	       NULL             AS SBOP_PolicyNumber,
	       NULL              AS RetroDate
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.dccuumbrellacommercialautostaging UMCA
	UNION ALL
	SELECT sessionid,
	       lineid,
	       UMBO.description  AS BO_Description,
	       UMBO.policynumber AS BO_PolicyNumber,
	       NULL             AS CA_Description,
	       NULL             AS CA_PolicyNumber,
	       NULL             AS EL_Description,
	       NULL             AS EL_PolicyNumber,
	       NULL             AS GL_Description,
	       NULL             AS GL_PolicyNumber,
	       NULL             AS SMART_Description,
	       NULL             AS SMART_PolicyNumber,
	       NULL             AS SBOP_Description,
	       NULL             AS SBOP_PolicyNumber,
	       NULL              AS RetroDate
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.dccuumbrellabusinessownersstaging UMBO
	UNION ALL
	SELECT UMGL.sessionid,
	       lineid,
	       NULL             AS BO_Description,
	       NULL             AS BO_PolicyNumber,
	       NULL             AS CA_Description,
	       NULL             AS CA_PolicyNumber,
	       NULL             AS EL_Description,
	       NULL             AS EL_PolicyNumber,
	       UMGL.description  AS GL_Description,
	       UMGL.policynumber AS GL_PolicyNumber,
	       NULL             AS SMART_Description,
	       NULL             AS SMART_PolicyNumber,
	       NULL             AS SBOP_Description,
	       NULL             AS SBOP_PolicyNumber,
	       WMGL.retrodate
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.dccuumbrellageneralliabilitystaging UMGL
	       LEFT JOIN @{pipeline().parameters.SOURCE_TABLE_OWNER}.wbcuumbrellageneralliabilitystaging WMGL
	              ON
	       UMGL.cu_umbrellageneralliabilityid = WMGL.cu_umbrellageneralliabilityid
	       AND UMGL.sessionid = WMGL.sessionid
	UNION ALL
	SELECT sessionid,
	       lineid,
	       NULL              AS BO_Description,
	       NULL              AS BO_PolicyNumber,
	       NULL              AS CA_Description,
	       NULL              AS CA_PolicyNumber,
	       NULL              AS EL_Description,
	       NULL              AS EL_PolicyNumber,
	       NULL              AS GL_Description,
	       NULL              AS GL_PolicyNumber,
	       WBSMT.description  AS SMART_Description,
	       WBSMT.policynumber AS SMART_PolicyNumber,
	       NULL              AS SBOP_Description,
	       NULL              AS SBOP_PolicyNumber,
	       NULL               AS RetroDate
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.wbcuumbrellasmartbusinessstage WBSMT
	UNION ALL
	SELECT sessionid,
	       lineid,
	       NULL               AS BO_Description,
	       NULL               AS BO_PolicyNumber,
	       NULL               AS CA_Description,
	       NULL               AS CA_PolicyNumber,
	       NULL               AS EL_Description,
	       NULL               AS EL_PolicyNumber,
	       NULL               AS GL_Description,
	       NULL               AS GL_PolicyNumber,
	       NULL               AS SMART_Description,
	       NULL               AS SMART_PolicyNumber,
	       WBSBOP.description  AS SBOP_Description,
	       WBSBOP.policynumber AS SBOP_PolicyNumber,
	       NULL                AS RetroDate
	FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.wbcuumbrellasbopstage WBSBOP  
	)
	select PT.PremiumTransactionID ,  wUPT.CoverageGUID, 
	DCCOVERAGE.BO_Description as BO_Description,
	DCCOVERAGE.BO_PolicyNumber as BO_PolicyNumber,
	DCCOVERAGE.CA_Description as CA_Description,
	DCCOVERAGE.CA_PolicyNumber as CA_PolicyNumber,
	DCCOVERAGE.EL_Description as EL_Description,
	DCCOVERAGE.EL_PolicyNumber as EL_PolicyNumber,
	DCCOVERAGE.GL_Description as GL_Description,
	DCCOVERAGE.GL_PolicyNumber as GL_PolicyNumber,
	DCCOVERAGE.SMART_Description as SMART_Description,
	DCCOVERAGE.SMART_PolicyNumber as SMART_PolicyNumber,
	DCCOVERAGE.SBOP_Description as SBOP_Description,
	DCCOVERAGE.SBOP_PolicyNumber as SBOP_PolicyNumber,
	DCCOVERAGE.RetroDate as RetroDate,
	CUPD.Million 
	FROM 
	@{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.PremiumTransaction PT  
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.WorkPremiumTransaction WPT
	on PT.PremiumTransactionAKID = WPT.PremiumTransactionAKId and PT.SourceSystemId='DCT' 
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTCoverageTransaction wUPT on WPT.PremiumTransactionStageId =wUPT.CoverageId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WorkDCTTransactionInsuranceLineLocationBridge a on a.CoverageId=wUPT.CoverageId
	left join DCCoverage on DCCoverage.LineId=a.LineId
	left join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WBCUPremiumDetailStage CUPD on wUPT.ParentCoverageObjectName='WB_CU_PremiumDetail'
	and CUPD.WBCUPremiumDetailId=wUPT.ParentCoverageObjectId
	where PT.CreatedDate>='@{pipeline().parameters.SELECTION_START_TS}'
	@{pipeline().parameters.WHERE_CLAUSE}
),
AGG_Umbrella AS (
	SELECT
	PremiumTransactionID,
	Id AS CoverageGUID,
	BO_Description,
	-- *INF*: MAX(BO_Description)
	MAX(BO_Description
	) AS o_BO_Description,
	BO_PolicyNumber,
	-- *INF*: MAX(BO_PolicyNumber)
	MAX(BO_PolicyNumber
	) AS o_BO_PolicyNumber,
	CA_Description,
	-- *INF*: MAX(CA_Description)
	-- 
	MAX(CA_Description
	) AS o_CA_Description,
	CA_PolicyNumber,
	-- *INF*: MAX(CA_PolicyNumber)
	MAX(CA_PolicyNumber
	) AS o_CA_PolicyNumber,
	EL_Description,
	-- *INF*: MAX(EL_Description)
	MAX(EL_Description
	) AS o_EL_Description,
	EL_PolicyNumber,
	-- *INF*: MAX(EL_PolicyNumber)
	MAX(EL_PolicyNumber
	) AS o_EL_PolicyNumber,
	GL_Description,
	-- *INF*: MAX(GL_Description)
	MAX(GL_Description
	) AS o_GL_Description,
	GL_PolicyNumber,
	-- *INF*: MAX(GL_PolicyNumber)
	MAX(GL_PolicyNumber
	) AS o_GL_PolicyNumber,
	SMART_Description,
	-- *INF*: MAX(SMART_Description)
	MAX(SMART_Description
	) AS o_SMART_Description,
	SMART_PolicyNumber,
	-- *INF*: max(SMART_PolicyNumber)
	max(SMART_PolicyNumber
	) AS o_SMART_PolicyNumber,
	SBOP_Description,
	-- *INF*: max(SBOP_Description)
	max(SBOP_Description
	) AS o_SBOP_Description,
	SBOP_PolicyNumber,
	-- *INF*: MAX(SBOP_PolicyNumber)
	MAX(SBOP_PolicyNumber
	) AS o_SBOP_PolicyNumber,
	RetroDate,
	-- *INF*: MAX(RetroDate)
	MAX(RetroDate
	) AS o_RetroDate,
	Million,
	-- *INF*: MAX(Million)
	MAX(Million
	) AS o_Million
	FROM SQ_DCCUUmbrella
	GROUP BY PremiumTransactionID
),
EXP_DefaultValue AS (
	SELECT
	PremiumTransactionID AS i_PremiumTransactionID,
	CoverageGUID AS i_CoverageGuid,
	o_BO_Description AS i_BO_Description,
	o_BO_PolicyNumber AS i_BO_PolicyNumber,
	o_CA_Description AS i_CA_Description,
	o_CA_PolicyNumber AS i_CA_PolicyNumber,
	o_EL_Description AS i_EL_Description,
	o_EL_PolicyNumber AS i_EL_PolicyNumber,
	o_GL_Description AS i_GL_Description,
	o_GL_PolicyNumber AS i_GL_PolicyNumber,
	o_SMART_Description AS i_SMART_Description,
	o_SMART_PolicyNumber AS i_SMART_PolicyNumber,
	o_SBOP_Description AS i_SBOP_Description,
	o_SBOP_PolicyNumber AS i_SBOP_PolicyNumber,
	o_RetroDate AS i_RetroActiveDate,
	o_Million AS i_Million,
	i_PremiumTransactionID AS o_PremiumTransactionID,
	1 AS o_CurrentSnapshotFlag,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: TO_DATE('01/01/1800 00:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_ExpirationDate,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	i_CoverageGuid AS o_CoverageGuid,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_EL_Description)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_EL_Description
	) AS o_UmbrellaEmployersLiabilityUnderlyingInsuranceCompanyName,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_EL_PolicyNumber)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_EL_PolicyNumber
	) AS o_UmbrellaEmployersLiabilityUnderlyingInsurancePolicyKey,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_BO_Description)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_BO_Description
	) AS o_UmbrellaBusinessOwnersUnderlyingInsuranceCompanyName,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_BO_PolicyNumber)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_BO_PolicyNumber
	) AS o_UmbrellaBusinessOwnersUnderlyingInsurancePolicyKey,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_GL_Description)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_GL_Description
	) AS o_UmbrellaGeneralLiabilityUnderlyingInsuranceCompanyName,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_GL_PolicyNumber)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_GL_PolicyNumber
	) AS o_UmbrellaGeneralLiabilityUnderlyingInsurancePolicyKey,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_CA_Description)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_CA_Description
	) AS o_UmbrellaCommercialAutoUnderlyingInsuranceCompanyName,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_CA_PolicyNumber)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_CA_PolicyNumber
	) AS o_UmbrellaCommercialAutoUnderlyingInsurancePolicyKey,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_SMART_Description)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_SMART_Description
	) AS o_UmbrellaSMARTbusinessUnderlyingInsuranceCompanyName,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_SMART_PolicyNumber)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_SMART_PolicyNumber
	) AS o_UmbrellaSMARTbusinessUnderlyingInsurancePolicyKey,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_SBOP_Description)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_SBOP_Description
	) AS o_UmbrellaSBOPUnderlyingInsuranceCompanyName,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_SBOP_PolicyNumber)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_SBOP_PolicyNumber
	) AS o_UmbrellaSBOPUnderlyingInsurancePolicyKey,
	'N/A' AS o_UmbrellaCoverageScope,
	-- *INF*: IIF(ISNULL(i_RetroActiveDate), TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS'), i_RetroActiveDate)
	IFF(i_RetroActiveDate IS NULL,
		TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
		),
		i_RetroActiveDate
	) AS o_RetroActiveDate,
	-- *INF*: IIF(ISNULL(i_Million), -1, i_Million)
	IFF(i_Million IS NULL,
		- 1,
		i_Million
	) AS o_UmbrellaLayer
	FROM AGG_Umbrella
),
LKP_CoverageDetailCommercialUmbrella AS (
	SELECT
	PremiumTransactionID,
	CoverageGuid,
	UmbrellaCoverageScope,
	RetroActiveDate,
	UmbrellaLayer
	FROM (
		SELECT 
			PremiumTransactionID,
			CoverageGuid,
			UmbrellaCoverageScope,
			RetroActiveDate,
			UmbrellaLayer
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialUmbrella
		WHERE SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PremiumTransactionID ORDER BY PremiumTransactionID) = 1
),
EXP_DetectChange AS (
	SELECT
	LKP_CoverageDetailCommercialUmbrella.PremiumTransactionID AS lkp_PremiumTransactionID,
	LKP_CoverageDetailCommercialUmbrella.CoverageGuid AS lkp_CoverageGuid,
	LKP_CoverageDetailCommercialUmbrella.UmbrellaCoverageScope AS lkp_UmbrellaCoverageScope,
	LKP_CoverageDetailCommercialUmbrella.RetroActiveDate AS lkp_RetroActiveDate,
	LKP_CoverageDetailCommercialUmbrella.UmbrellaLayer AS lkp_UmbrellaLayer,
	EXP_DefaultValue.o_PremiumTransactionID AS PremiumTransactionID,
	EXP_DefaultValue.o_CurrentSnapshotFlag AS CurrentSnapshotFlag,
	EXP_DefaultValue.o_AuditID AS AuditID,
	EXP_DefaultValue.o_EffectiveDate AS EffectiveDate,
	EXP_DefaultValue.o_ExpirationDate AS ExpirationDate,
	EXP_DefaultValue.o_SourceSystemID AS SourceSystemID,
	EXP_DefaultValue.o_CreatedDate AS CreatedDate,
	EXP_DefaultValue.o_ModifiedDate AS ModifiedDate,
	EXP_DefaultValue.o_CoverageGuid AS CoverageGuid,
	EXP_DefaultValue.o_UmbrellaEmployersLiabilityUnderlyingInsuranceCompanyName AS UmbrellaEmployersLiabilityUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_UmbrellaEmployersLiabilityUnderlyingInsurancePolicyKey AS UmbrellaEmployersLiabilityUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.o_UmbrellaBusinessOwnersUnderlyingInsuranceCompanyName AS UmbrellaBusinessOwnersUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_UmbrellaBusinessOwnersUnderlyingInsurancePolicyKey AS UmbrellaBusinessOwnersUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.o_UmbrellaGeneralLiabilityUnderlyingInsuranceCompanyName AS UmbrellaGeneralLiabilityUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_UmbrellaGeneralLiabilityUnderlyingInsurancePolicyKey AS UmbrellaGeneralLiabilityUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.o_UmbrellaCommercialAutoUnderlyingInsuranceCompanyName AS UmbrellaCommercialAutoUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_UmbrellaCommercialAutoUnderlyingInsurancePolicyKey AS UmbrellaCommercialAutoUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.o_UmbrellaSMARTbusinessUnderlyingInsuranceCompanyName AS UmbrellaSMARTbusinessUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_UmbrellaSMARTbusinessUnderlyingInsurancePolicyKey AS UmbrellaSMARTbusinessUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.o_UmbrellaSBOPUnderlyingInsuranceCompanyName AS UmbrellaSBOPUnderlyingInsuranceCompanyName,
	EXP_DefaultValue.o_UmbrellaSBOPUnderlyingInsurancePolicyKey AS UmbrellaSBOPUnderlyingInsurancePolicyKey,
	EXP_DefaultValue.o_UmbrellaCoverageScope AS UmbrellaCoverageScope,
	EXP_DefaultValue.o_RetroActiveDate AS RetroActiveDate,
	EXP_DefaultValue.o_UmbrellaLayer AS UmbrellaLayer,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_PremiumTransactionID), 'New', 
	-- lkp_CoverageGuid<>CoverageGuid
	-- or lkp_UmbrellaCoverageScope<>UmbrellaCoverageScope
	-- or lkp_RetroActiveDate<>RetroActiveDate
	-- or lkp_UmbrellaLayer<>UmbrellaLayer,  'Update',
	-- 'No Change'
	-- ) 
	-- 
	DECODE(TRUE,
		lkp_PremiumTransactionID IS NULL, 'New',
		lkp_CoverageGuid <> CoverageGuid 
		OR lkp_UmbrellaCoverageScope <> UmbrellaCoverageScope 
		OR lkp_RetroActiveDate <> RetroActiveDate 
		OR lkp_UmbrellaLayer <> UmbrellaLayer, 'Update',
		'No Change'
	) AS v_ChangeFlag,
	v_ChangeFlag AS o_ChangeFlag
	FROM EXP_DefaultValue
	LEFT JOIN LKP_CoverageDetailCommercialUmbrella
	ON LKP_CoverageDetailCommercialUmbrella.PremiumTransactionID = EXP_DefaultValue.o_PremiumTransactionID
),
FIL_Records AS (
	SELECT
	PremiumTransactionID, 
	CurrentSnapshotFlag, 
	AuditID, 
	EffectiveDate, 
	ExpirationDate, 
	SourceSystemID, 
	CreatedDate, 
	ModifiedDate, 
	CoverageGuid, 
	UmbrellaCoverageScope, 
	RetroActiveDate, 
	UmbrellaLayer, 
	o_ChangeFlag AS ChangeFlag
	FROM EXP_DetectChange
	WHERE ChangeFlag='New'
),
TGT_CoverageDetailCommercialUmbrella_Insert AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailCommercialUmbrella
	(PremiumTransactionID, CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, CoverageGuid, UmbrellaCoverageScope, RetroactiveDate, UmbrellaLayer)
	SELECT 
	PREMIUMTRANSACTIONID, 
	CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	COVERAGEGUID, 
	UMBRELLACOVERAGESCOPE, 
	RetroActiveDate AS RETROACTIVEDATE, 
	UMBRELLALAYER
	FROM FIL_Records
),
SQ_CoverageDetailCommercialUmbrella AS (
	SELECT 
	CDCUPrevious.UmbrellaCoverageScope,
	CDCUPrevious.RetroactiveDate,
	CDCUPrevious.UmbrellaLayer,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL
	inner join CoverageDetailCommercialUmbrella CDCUPrevious
	on ( CDCUPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailCommercialUmbrella CDCUToUpdate
	on ( CDCUToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionID)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL.premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Offset'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	  CDCUPrevious.RetroactiveDate <> CDCUToUpdate.RetroactiveDate
	  OR CDCUPrevious.UmbrellaCoverageScope <> CDCUToUpdate.UmbrellaCoverageScope
	  OR CDCUPrevious.UmbrellaLayer <> CDCUToUpdate.UmbrellaLayer
	  )
),
Exp_CoveargedetailCommercialUmbrella AS (
	SELECT
	UmbrellaCoverageScope,
	RetroactiveDate,
	UmbrellaLayer,
	wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate
	FROM SQ_CoverageDetailCommercialUmbrella
),
UPD_CoveragedetailCommercialUmbrella AS (
	SELECT
	UmbrellaCoverageScope, 
	RetroactiveDate, 
	UmbrellaLayer, 
	wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate
	FROM Exp_CoveargedetailCommercialUmbrella
),
TGT_CoverageDetailCommercialUmbrella_Upd_Offsets AS (
	MERGE INTO CoverageDetailCommercialUmbrella AS T
	USING UPD_CoveragedetailCommercialUmbrella AS S
	ON T.PremiumTransactionID = S.wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.UmbrellaCoverageScope = S.UmbrellaCoverageScope, T.RetroactiveDate = S.RetroactiveDate, T.UmbrellaLayer = S.UmbrellaLayer
),
SQ_CoverageDetailCommercialUmbrella_Deprecated AS (
	SELECT 
	CDCUPrevious.UmbrellaCoverageScope,
	CDCUPrevious.RetroactiveDate,
	CDCUPrevious.UmbrellaLayer,
	WPTOL.PremiumTransactionID AS Wrk_PremiumTransactionID
	FROM
	WorkPremiumTransactionOffsetLineage WPTOL
	inner join CoverageDetailCommercialUmbrella CDCUPrevious
	on ( CDCUPrevious.PremiumTransactionID= WPTOL.previouspremiumtransactionid)
	inner join CoverageDetailCommercialUmbrella CDCUToUpdate
	on ( CDCUToUpdate.PremiumTransactionID= WPTOL.PremiumTransactionID)
	INNER JOIN premiumtransaction pt WITH (NOLOCK) on
		WPTOL.premiumtransactionid=pt.premiumtransactionid and PT.OffsetOnsetCode='Deprecated'
	WHERE
	WPTOL.UpdateAttributeFlag = 1 
	AND (
	  CDCUPrevious.RetroactiveDate <> CDCUToUpdate.RetroactiveDate
	  OR CDCUPrevious.UmbrellaCoverageScope <> CDCUToUpdate.UmbrellaCoverageScope
	  OR CDCUPrevious.UmbrellaLayer <> CDCUToUpdate.UmbrellaLayer
	  )
),
Exp_CoveargedetailCommercialUmbrella_Deprecated AS (
	SELECT
	UmbrellaCoverageScope,
	RetroactiveDate,
	UmbrellaLayer,
	wrk_PremiumTransactionID,
	SYSDATE AS o_ModifiedDate
	FROM SQ_CoverageDetailCommercialUmbrella_Deprecated
),
UPD_CoveragedetailCommercialUmbrella_Deprecated AS (
	SELECT
	UmbrellaCoverageScope, 
	RetroactiveDate, 
	UmbrellaLayer, 
	wrk_PremiumTransactionID, 
	o_ModifiedDate AS ModifiedDate
	FROM Exp_CoveargedetailCommercialUmbrella_Deprecated
),
TGT_CoverageDetailCommercialUmbrella_Upd_Deprecated AS (
	MERGE INTO CoverageDetailCommercialUmbrella AS T
	USING UPD_CoveragedetailCommercialUmbrella_Deprecated AS S
	ON T.PremiumTransactionID = S.wrk_PremiumTransactionID
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.ModifiedDate = S.ModifiedDate, T.UmbrellaCoverageScope = S.UmbrellaCoverageScope, T.RetroactiveDate = S.RetroactiveDate, T.UmbrellaLayer = S.UmbrellaLayer
),