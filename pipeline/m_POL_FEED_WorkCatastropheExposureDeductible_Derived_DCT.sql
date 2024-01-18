WITH
SQ_WorkCatastropheExposureTransaction AS (
	select T.PolicyKey,
	T.LocationNumber,
	D.DeductibleValue from WorkCatastropheExposureTransaction T inner join WorkCatastropheExposureDeductible D
	on 
	D.PolicyKey = T.PolicyKey and D.BusinessType='SBOP' 
	and D.DeductibleType in ('BusinessPersonalPropertyStandard','BuildingStandard')
	where 
	T.BusinessType = 'Commercial Inland Marine'
	and T.ProductDescription = 'SBOP' 
	and T.CoverageDescription='Excess Personal Computer Coverage/EDP'
),
Agg_DeductibleValue AS (
	SELECT
	PolicyKey,
	LocationNumber,
	DeductibleValue,
	-- *INF*: MIN(DeductibleValue)
	MIN(DeductibleValue) AS o_DeductibleValue
	FROM SQ_WorkCatastropheExposureTransaction
	GROUP BY PolicyKey, LocationNumber
),
Exp_PassThrough AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS AuditId,
	Sysdate AS CreatedDate,
	Sysdate AS ModifiedDate,
	'DCT' AS SourceSystemId,
	PolicyKey,
	LocationNumber,
	'Commercial Inland Marine' AS o_BusinessType,
	'Excess Personal Computer Coverage/EDP' AS o_DeductibleType,
	o_DeductibleValue AS DeductibleValue
	FROM Agg_DeductibleValue
),
WorkCatastropheExposureDeductible1 AS (
	INSERT INTO WorkCatastropheExposureDeductible
	(AuditId, CreatedDate, ModifiedDate, SourceSystemId, PolicyKey, LocationNumber, BusinessType, DeductibleType, DeductibleValue)
	SELECT 
	AUDITID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	SOURCESYSTEMID, 
	POLICYKEY, 
	LOCATIONNUMBER, 
	o_BusinessType AS BUSINESSTYPE, 
	o_DeductibleType AS DEDUCTIBLETYPE, 
	DEDUCTIBLEVALUE
	FROM Exp_PassThrough
),