WITH
SQ_ISOPropertyCauseOfLoss AS (

-- TODO Manual --

),
EXP_METADATA AS (
	SELECT
	SourceSystem AS i_SourceSystem,
	ProductCode AS i_ProductCode,
	MajorPerilCode AS i_MajorPerilCode,
	Subline AS i_Subline,
	ISOPropertyCauseOfLossGroup AS i_ISOPropertyCauseOfLossGroup,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	-- *INF*: TO_DATE('01/01/1800 01:00:00','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('01/01/1800 01:00:00', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59','MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_ExpirationDate,
	i_SourceSystem AS o_SourceSystemID,
	i_ProductCode AS o_ProductCode,
	i_MajorPerilCode AS o_MajorPerilCode,
	i_Subline AS o_SublineCode,
	-- *INF*: LTRIM(RTRIM(i_ISOPropertyCauseOfLossGroup))
	LTRIM(RTRIM(i_ISOPropertyCauseOfLossGroup
		)
	) AS o_ISOCommercialPropertyCauseOfLossGroup
	FROM SQ_ISOPropertyCauseOfLoss
),
SupISOCommercialPropertyCauseOfLossGroup_IR AS (
	TRUNCATE TABLE SupISOCommercialPropertyCauseOfLossGroup;
	INSERT INTO SupISOCommercialPropertyCauseOfLossGroup
	(AuditId, CreatedDate, ModifiedDate, EffectiveDate, ExpirationDate, SourceSystemID, ProductCode, MajorPerilCode, SublineCode, ISOCommercialPropertyCauseOfLossGroup)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_SourceSystemID AS SOURCESYSTEMID, 
	o_ProductCode AS PRODUCTCODE, 
	o_MajorPerilCode AS MAJORPERILCODE, 
	o_SublineCode AS SUBLINECODE, 
	o_ISOCommercialPropertyCauseOfLossGroup AS ISOCOMMERCIALPROPERTYCAUSEOFLOSSGROUP
	FROM EXP_METADATA
),