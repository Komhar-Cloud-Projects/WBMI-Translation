WITH
SQ_CoverageDetailCrime AS (
	SELECT cdc.CoverageGUID,
	cdd.CoverageDetailDimId,
	cdd.EffectiveDate,
	cdd.ExpirationDate,
	cdc.IndustryGroup as IndustryGroup
	from CoverageDetailCrime CDC
	inner join @{pipeline().parameters.TARGET_DATABASE_NAME}.@{pipeline().parameters.TARGET_TABLE_OWNER}.CoverageDetailDim CDD
	on CDC.PremiumTransactionID=CDD.EDWPremiumTransactionPKId
	inner join PremiumTransaction p
	on CDC.PremiumTransactionID=p.PremiumTransactionID
	where CDC.ModifiedDate>=@{pipeline().parameters.SELECTION_START_TS}
),
LKP_CDCD AS (
	SELECT
	CoverageDetailDimId,
	CoverageGUID,
	IndustryGroup,
	i_CoverageDetailDimId
	FROM (
		SELECT 
			CoverageDetailDimId,
			CoverageGUID,
			IndustryGroup,
			i_CoverageDetailDimId
		FROM CoverageDetailCrimeDim
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY CoverageDetailDimId ORDER BY CoverageDetailDimId) = 1
),
EXP_CalValues AS (
	SELECT
	LKP_CDCD.CoverageDetailDimId AS lkp_CoverageDetailDimId,
	LKP_CDCD.CoverageGUID AS lkp_CoverageGUID,
	LKP_CDCD.IndustryGroup AS lkp_CrimeIndustryGroup,
	SQ_CoverageDetailCrime.CoverageDetailDimId AS i_CoverageDetailDimId,
	SQ_CoverageDetailCrime.CoverageGUID AS i_CoverageGUID,
	SQ_CoverageDetailCrime.IndustryGroup AS i_CrimeIndustryGroup,
	SQ_CoverageDetailCrime.EffectiveDate AS i_EffectiveDate,
	SQ_CoverageDetailCrime.ExpirationDate AS i_ExpirationDate,
	i_CoverageDetailDimId AS o_CoverageDetailDimId,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	i_EffectiveDate AS o_EffectiveDate,
	i_ExpirationDate AS o_ExpirationDate,
	sysdate AS o_CreatedDate,
	sysdate AS o_ModifiedDate,
	i_CoverageGUID AS o_CoverageGUID,
	i_CrimeIndustryGroup AS o_CrimeIndustryGroup,
	-- *INF*: DECODE(TRUE,
	-- ISNULL(lkp_CoverageDetailDimId),
	-- 'INSERT',
	-- i_CrimeIndustryGroup <>lkp_CrimeIndustryGroup
	-- OR i_CoverageGUID<>lkp_CoverageGUID,
	-- 'UPDATE',
	-- 'NOCHANGE')
	DECODE(TRUE,
		lkp_CoverageDetailDimId IS NULL, 'INSERT',
		i_CrimeIndustryGroup <> lkp_CrimeIndustryGroup OR i_CoverageGUID <> lkp_CoverageGUID, 'UPDATE',
		'NOCHANGE') AS o_changeflag
	FROM SQ_CoverageDetailCrime
	LEFT JOIN LKP_CDCD
	ON LKP_CDCD.CoverageDetailDimId = SQ_CoverageDetailCrime.CoverageDetailDimId
),
RTRTRANS AS (
	SELECT
	o_CoverageDetailDimId,
	o_AuditId,
	o_EffectiveDate,
	o_ExpirationDate,
	o_CreatedDate,
	o_ModifiedDate,
	o_CoverageGUID,
	o_CrimeIndustryGroup,
	o_changeflag
	FROM EXP_CalValues
),
RTRTRANS_INSERT AS (SELECT * FROM RTRTRANS WHERE o_changeflag='INSERT'),
RTRTRANS_UPDATE AS (SELECT * FROM RTRTRANS WHERE o_changeflag='UPDATE'),
CoverageDetailCrimeDim_INSERT AS (
	INSERT INTO CoverageDetailCrimeDim
	(CoverageDetailDimId, AuditID, EffectiveDate, ExpirationDate, CreatedDate, ModifiedDate, CoverageGuid, IndustryGroup)
	SELECT 
	o_CoverageDetailDimId AS COVERAGEDETAILDIMID, 
	o_AuditId AS AUDITID, 
	o_EffectiveDate AS EFFECTIVEDATE, 
	o_ExpirationDate AS EXPIRATIONDATE, 
	o_CreatedDate AS CREATEDDATE, 
	o_ModifiedDate AS MODIFIEDDATE, 
	o_CoverageGUID AS COVERAGEGUID, 
	o_CrimeIndustryGroup AS INDUSTRYGROUP
	FROM RTRTRANS_INSERT
),
UPD_CDCD AS (
	SELECT
	o_CoverageDetailDimId AS o_CoverageDetailDimId3, 
	o_EffectiveDate AS o_EffectiveDate3, 
	o_ExpirationDate AS o_ExpirationDate3, 
	o_ModifiedDate AS o_ModifiedDate3, 
	o_CoverageGUID AS o_CoverageGUID3, 
	o_CrimeIndustryGroup AS o_CrimeIndustryGroup3
	FROM RTRTRANS_UPDATE
),
CoverageDetailCrimeDim_UPDATE AS (
	MERGE INTO CoverageDetailCrimeDim AS T
	USING UPD_CDCD AS S
	ON T.CoverageDetailDimId = S.o_CoverageDetailDimId3
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.EffectiveDate = S.o_EffectiveDate3, T.ExpirationDate = S.o_ExpirationDate3, T.ModifiedDate = S.o_ModifiedDate3, T.CoverageGuid = S.o_CoverageGUID3, T.IndustryGroup = S.o_CrimeIndustryGroup3
),