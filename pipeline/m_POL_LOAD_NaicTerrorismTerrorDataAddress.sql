WITH
SQ_TerrorData AS (
	SELECT 
	DISTINCT
	a.WorkNAICTerrorismSubTotalId as WorkNAICTerrorismSubTotalId,
	a.CreatedDate as CreatedDate,
	a.ModifiedDate as ModifiedDate,
	a.polkey as polkey,
	a.RatingStatCoverageID as RatingStatCoverageID
	,a.SourceSystemID as SourceSystemID
	,a.TableCode as TableCode
	,a.Year as Year
	,a.AuditId as AuditId
	,a.Stabbr as Stabbr
	,a.CoCode as CoCode
	,a.CoType as CoType
	,a.Zip as Zip
	,a.PolCat as PolCat
	,a.Coverage as Coverage
	,a.IndCodeType as IndCodeType
	,a.Code as Code
	,a.PolType as PolType
	,a.CovType as CovType
	,a.PolNum as PolNum
	,a.EstNum as EstNum
	,a.Limit as Limit
	,a.DirectWrittenPremium as DirectWrittenPremium
	,a.EarnedPremium as EarnedPremium
	,a.TableCodeInsuranceLine as TableCodeInsuranceLine
	,a.ReinsurancePercent as ReinsurancePercent
	,a.ReinsuranceEarnedPremium as ReinsuranceEarnedPremium
	,a.TerrorismRiskInd as TerrorismRiskInd
	,a.BlackListCoverageFlag as BlackListCoverageFlag
	,a.CoverageLimitValueGLStateOverride as CoverageLimitValueGLStateOverride
	,a.LimitHashKey as LimitHashKey
	,a.Deductible as Deductible
	,a.LOB as LOB
	,a.InsuranceLineCode as InsuranceLineCode
	,a.CoverageCode as CoverageCode
	,t.ratingcity as ratingcity
	,t.ratingcounty as ratingcounty,
	t.streetaddress as streetaddress,
	t.locationnumber as locationnumber,
	t.sublocationunitnumber as sublocationunitnumber,
	a.[CoverageCodeBreakup] as CoverageCodeBreakup
	FROM 
	datafeedmart.dbo.TerrorData A 
	INNER JOIN  
	(
	SELECT 
	DISTINCT
	ratingcoverageakid,
	pol_key,
	RiskLocationAKID,
	coveragecode,
	BlackListCoverageFlag,
	sourcesystemid,
	PolicyEffectiveDateYear,
	ratingcity,
	ratingcounty,
	streetaddress,
	locationnumber,
	sublocationunitnumber,
	auditid
	FROM
	datafeedmart.dbo.WorkNAICTerrorismControl
	WHERE 
	SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	AND
	PolicyEffectiveDateYear='@{pipeline().parameters.YEAR}'
	AND
	auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	) t ON 
	A.RatingStatCoverageID=t.ratingcoverageakid and 
	A.polkey=t.pol_key and
	A.MaxRiskLocationAKID = t.RiskLocationAKID and 
	a.coveragecode= t.coveragecode and 
	a.BlackListCoverageFlag=t.BlackListCoverageFlag and 
	a.SourceSystemId=t.sourcesystemid and 
	a.Year=t.PolicyEffectiveDateYear and
	a.auditid=t.auditid
	WHERE
	A.SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}' AND 
	a.year='@{pipeline().parameters.YEAR}' AND
	a.auditid = @{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID}
	@{pipeline().parameters.WHERE}
),
EXPTRANS AS (
	SELECT
	WorkNAICTerrorismSubTotalId,
	CreatedDate,
	ModifiedDate,
	PolKey,
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
	LOB,
	InsuranceLineCode,
	CoverageCode,
	RatingCity,
	RatingCounty,
	StreetAddress,
	LocationNumber,
	SubLocationUnitNumber,
	CoverageCodeBreakup
	FROM SQ_TerrorData
),
TerrorDataAddress AS (

	------------ PRE SQL ----------
	delete from TerrorDataAddress where AuditId=@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} and Year=@{pipeline().parameters.YEAR}
	-------------------------------


	INSERT INTO TerrorDataAddress
	(WorkNAICTerrorismSubTotalId, CreatedDate, ModifiedDate, PolKey, RatingStatCoverageID, SourceSystemID, TableCode, Year, AuditId, Stabbr, CoCode, CoType, Zip, PolCat, Coverage, IndCodeType, Code, PolType, CovType, PolNum, EstNum, Limit, DirectWrittenPremium, EarnedPremium, TableCodeInsuranceLine, ReinsurancePercent, ReinsuranceEarnedPremium, TerrorismRiskInd, BlackListCoverageFlag, CoverageLimitValueGLStateOverride, LimitHashKey, Deductible, LOB, InsuranceLineCode, CoverageCode, RatingCity, RatingCounty, StreetAddress, LocationNumber, SubLocationUnitNumber, CoverageCodeBreakup)
	SELECT 
	WORKNAICTERRORISMSUBTOTALID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLKEY, 
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
	LOB, 
	INSURANCELINECODE, 
	COVERAGECODE, 
	RATINGCITY, 
	RATINGCOUNTY, 
	STREETADDRESS, 
	LOCATIONNUMBER, 
	SUBLOCATIONUNITNUMBER, 
	COVERAGECODEBREAKUP
	FROM EXPTRANS
),