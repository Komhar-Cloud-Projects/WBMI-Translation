WITH
SQ_WorkNAICTerrorismSubTotal AS (
	SELECT 
	                     t.WorkNAICTerrorismSubTotalId as WorkNAICTerrorismSubTotalId, 
	                     t.CreatedDate as CreatedDate, 
	                     t.ModifiedDate as ModifiedDate, 
	                     t.pol_key as pol_key, 
	                     t.RatingStatCoverageID as RatingStatCoverageID,
	                     t.SourceSystemID as SourceSystemID, 
	                     t.TableCode as TableCode,
	                     t.[Year] as Year,
				  t.AuditId as AuditId,
	                     t.Stabbr as Stabbr, 
	                     t.CoCode as CoCode, 
	                     t.CoType as CoType, 
	                     t.Zip as Zip, 
	                     t.PolCat as PolCat, 
	                     t.Coverage as Coverage, 
	                     t.IndCodeType as IndCodeType, 
	                     t.Code as Code, 
	                     t.PolType as PolType, 
	                     t.CovType as CovType, 
	                     t.PolNum as PolNum, 
	                     t.EstNum as EstNum, 
	                     t.Limit as Limit, 
	                     t.DirectWrittenPremium as DirectWrittenPremium, 
	                     t.EarnedPremium as EarnedPremium, 
	                     t.TableCodeInsuranceLine as TableCodeInsuranceLine, 
	                     t.ReinsurancePercent as ReinsurancePercent, 
	                     t.ReinsuranceEarnedPremium as ReinsuranceEarnedPremium, 
	                     t.TerrorismRiskInd as TerrorismRiskInd, 
	                     t.BlackListCoverageFlag as BlackListCoverageFlag, 
	                     t.CoverageLimitValueGLStateOverride as CoverageLimitValueGLStateOverride, 
	                     t.LimitHashKey as LimitHashKey,
	                     CASE
	                     WHEN c.CoverageCode IN ('BRDLBL','CONDOMISC','FOODCN','ORDLC','RPTFORM','SEWER','USERVDDBR','USERVDDPO','UTILSERVBLDG','UTILSERVPP', 'UTILSERVTE','VACPMT') THEN 0
	                     WHEN c.CoverageCode = 'TRIA' and c.ProductCode = '430' and c.PolicyOfferingCode='430' and c.InsuranceReferenceLineOfBusinessCode='300' THEN 0
	                     WHEN c.CoverageCode = 'TRIA' and c.ProductCode = '300' THEN 0
	                     WHEN c.CoverageCode = 'TRIA' and c.ProductCode = '450' and c.PolicyOfferingCode='450' and c.InsuranceReferenceLineOfBusinessCode in ('300','500') THEN 0
	                     ELSE t.Deductible
	                     END AS Deductible,
	                     c.Lob as Lob,
	                     c.InsuranceLineCode as InsuranceLineCode, 
	                     c.CoverageCode as CoverageCode,
				 c.MaxRiskLocationAKID as MaxRiskLocationAKID,
				 CASE WHEN CHARINDEX('-', c.CoverageCode) > 0 then
				   LEFT(c.CoverageCode, CHARINDEX('-', c.CoverageCode) - 1) ELSE c.CoverageCode END as CoverageCodeBreakup
	FROM         
		 dbo.WorkNAICTerrorismSubTotal AS t WITH (nolock) 
	       INNER JOIN
	(
	select
	StatisticalCoverageAKID,
	RatingCoverageAKId,
	SourceSystemID,
	pol_key,
	InsuranceLineCode,
	CoverageCode,
	ZipPostalCode,
	PolicyEffectiveDateYear,
	max(RiskLocationAKID) as MaxRiskLocationAKID,
	case when AslNum = '2' THEN '02' ELSE lob END as Lob,
	AuditId,
	ProductCode,
	PolicyOfferingCode,
	InsuranceReferenceLineOfBusinessCode
	FROM
	WorkNAICTerrorismControl 
	WHERE 
	SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND 
	PolicyEffectiveDateYear='@{pipeline().parameters.YEAR}' AND
	Auditid=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	GROUP BY
	StatisticalCoverageAKID,
	RatingCoverageAKId,
	SourceSystemID,
	pol_key,
	InsuranceLineCode,
	CoverageCode,
	ZipPostalCode,
	PolicyEffectiveDateYear,
	case when AslNum = '2' THEN '02' ELSE lob END,
	AuditId,
	ProductCode,
	PolicyOfferingCode,
	InsuranceReferenceLineOfBusinessCode
	) c on 
	t.SourceSystemID=C.SourceSystemID and 
	t.RatingStatCoverageID=C.RatingCoverageAKId and
	t.Year=C.PolicyEffectiveDateYear and 
	t.pol_key=C.pol_key and 
	t.zip=C.ZipPostalCode and
	t.AuditId=C.AuditId
	WHERE 
	t.Year='@{pipeline().parameters.YEAR}' AND 
	t.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND
	t.auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE}
),
EXPTRANS AS (
	SELECT
	WorkNAICTerrorismSubTotalId,
	CreatedDate,
	ModifiedDate,
	pol_key,
	RatingStatCoverageID,
	SourceSystemID,
	TableCode,
	Year,
	AuditId,
	Stabbr,
	CoCode,
	CoType,
	Zip,
	PolCat,
	Coverage,
	IndCodeType,
	Code,
	PolType,
	CovType,
	PolNum,
	EstNum,
	Limit,
	DirectWrittenPremium,
	EarnedPremium,
	TableCodeInsuranceLine,
	ReinsurancePercent,
	ReinsuranceEarnedPremium,
	TerrorismRiskInd,
	BlackListCoverageFlag,
	CoverageLimitValueGLStateOverride,
	LimitHashKey,
	Deductible,
	Lob,
	InsuranceLineCode,
	CoverageCode,
	MaxRiskLocationAKID,
	CoverageCodeBreakup
	FROM SQ_WorkNAICTerrorismSubTotal
),
TerrorData AS (

	------------ PRE SQL ----------
	delete from TerrorData where AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and Year=@{pipeline().parameters.YEAR}
	-------------------------------


	INSERT INTO TerrorData
	(WorkNAICTerrorismSubTotalId, CreatedDate, ModifiedDate, PolKey, RatingStatCoverageID, SourceSystemID, TableCode, Year, AuditId, Stabbr, CoCode, CoType, Zip, PolCat, Coverage, IndCodeType, Code, PolType, CovType, PolNum, EstNum, Limit, DirectWrittenPremium, EarnedPremium, TableCodeInsuranceLine, ReinsurancePercent, ReinsuranceEarnedPremium, TerrorismRiskInd, BlackListCoverageFlag, CoverageLimitValueGLStateOverride, LimitHashKey, Deductible, LOB, InsuranceLineCode, CoverageCode, MaxRiskLocationAKID, CoverageCodeBreakup)
	SELECT 
	WORKNAICTERRORISMSUBTOTALID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	pol_key AS POLKEY, 
	RATINGSTATCOVERAGEID, 
	SOURCESYSTEMID, 
	TABLECODE, 
	YEAR, 
	AUDITID, 
	STABBR, 
	COCODE, 
	COTYPE, 
	ZIP, 
	POLCAT, 
	COVERAGE, 
	INDCODETYPE, 
	CODE, 
	POLTYPE, 
	COVTYPE, 
	POLNUM, 
	ESTNUM, 
	LIMIT, 
	DIRECTWRITTENPREMIUM, 
	EARNEDPREMIUM, 
	TABLECODEINSURANCELINE, 
	REINSURANCEPERCENT, 
	REINSURANCEEARNEDPREMIUM, 
	TERRORISMRISKIND, 
	BLACKLISTCOVERAGEFLAG, 
	COVERAGELIMITVALUEGLSTATEOVERRIDE, 
	LIMITHASHKEY, 
	DEDUCTIBLE, 
	Lob AS LOB, 
	INSURANCELINECODE, 
	COVERAGECODE, 
	MAXRISKLOCATIONAKID, 
	COVERAGECODEBREAKUP
	FROM EXPTRANS
),