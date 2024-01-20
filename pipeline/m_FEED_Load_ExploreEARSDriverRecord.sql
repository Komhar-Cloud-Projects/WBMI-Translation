WITH
SQ_Driver AS (
	Declare @StartTime datetime
	Declare @EndTime datetime
	
	set @StartTime=@{pipeline().parameters.START_DATE}
	set @EndTime=@{pipeline().parameters.END_DATE}
	
	select distinct DP.PolicyNumber pol_num,
	WP.PolicyVersionFormatted pol_mod,
	DP.ExpirationDate pol_exp_date,
	WA.Reference AgencyCode,
	case when len(ltrim(rtrim(ISNULL(DCD.StateLicensed,'N/A'))))=0 then 'N/A' else ISNULL(DCD.StateLicensed,'N/A') end LicenseState,
	case when len(ltrim(rtrim(ISNULL(DCD.DriversLicenseNumber,'N/A'))))=0 then 'N/A' else ISNULL(DCD.DriversLicenseNumber,'N/A') end LicenseNumber,
	case when len(ltrim(rtrim(ISNULL(WCD.LastName,'N/A'))))=0 then 'N/A' else ISNULL(SUBSTRING(WCD.LastName,1,25),'N/A') end LastName,
	case when len(ltrim(rtrim(ISNULL(WCD.Name,'N/A'))))=0 then 'N/A' else ISNULL(SUBSTRING(WCD.Name,1,20),'N/A') end FirstName,
	case when len(ltrim(rtrim(ISNULL(WCD.MiddleInitial,'N/A'))))=0 then 'N/A' else ISNULL(SUBSTRING(WCD.MiddleInitial,1,20),'N/A') end MiddleName,
	case when len(ltrim(rtrim(ISNULL(substring(WCD.Gender,1,1),'N/A'))))=0 then 'N/A' else ISNULL(substring(WCD.Gender,1,1),'U') end  GenderCode,
	WCD.DateOfBirth Birthdate,
	case when len(ltrim(rtrim(ISNULL(dl.Address1,'N/A'))))=0 then 'N/A' else ISNULL(dl.Address1,'N/A') end addr_line_1,
	case when len(ltrim(rtrim(ISNULL(DL.PostalCode,'N/A'))))=0 then 'N/A' else ISNULL(substring(DL.PostalCode,1,5),'N/A') end zip_postal_code
	from @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Policy DP
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy WP
	on DP.SessionId=WP.SessionId
	and DP.PolicyId=WP.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction T
	on DP.SessionId=T.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Line L
	on DP.SessionId=L.SessionId
	and DP.PolicyId=L.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_CA_Driver DCD
	on DP.SessionId=DCD.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_CA_Driver WCD
	on DCD.SessionId=WCD.SessionId
	and DCD.CA_DriverId=WCD.CA_DriverId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Party DCP
	on DP.SessionId=DCP.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Location DL
	on DP.SessionId=DL.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_LocationAssociation DLA
	on DL.SessionId=DLA.SessionId
	and DL.LocationId=DLA.LocationId
	and DLA.LocationAssociationType='Account'
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Agency WA
	on DCP.SessionId=WA.SessionId
	and DCP.PartyId=WA.PartyId
	where 
	T.HistoryID in (select max(C.HistoryID) HistoryID from DC_Policy A
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.WB_Policy B
	on A.SessionId=B.SessionId
	and A.PolicyId=B.PolicyId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction C
	on A.SessionId=C.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Session S
	on A.SessionId=S.SessionId
	inner join @{pipeline().parameters.SOURCE_TABLE_OWNER}.DC_Transaction T
	on A.SessionId=T.SessionId
	where C.State='Committed'
	and case when S.CreateDateTime<=T.TransactionDate then S.CreateDateTime else T.TransactionDate end<=@StartTime
	group by B.PolicyNumber,B.PolicyVersionFormatted)
	--and DCD.StateLicensed in ('IA','MI','MN','OH','WI','MO','KS','IL','IN')
	--and DP.PrimaryRatingState in ('IL','IN','IA','KS','MI','MN','MO','OH','WI')
	and WCD.ExcludeDriver = 'No' 
	and WCD.MVRStatus in ('Cleared','Returned')
	and DP.Status not in ('Quote','PolicyDeclined','PolicyClosed','Bound','Application')
	and wp.Division<>'NSI'
	and case when DP.Status='Cancelled' and T.EffectiveDate<=@StartTime then 1 else 0 end=0
	and L.Type='CommercialAuto'
	and @StartTime between DP.EffectiveDate and DP.ExpirationDate
	@{pipeline().parameters.WHERE_CLAUSE}
),
EXP_Calculate AS (
	SELECT
	pol_num AS i_pol_num,
	pol_mod AS i_pol_mod,
	pol_exp_date AS i_pol_exp_date,
	AgencyCode AS i_AgencyCode,
	LicenseState AS i_LicenseState,
	LicenseNumber AS i_LicenseNumber,
	LastName AS i_LastName,
	FirstName AS i_FirstName,
	MiddleName AS i_MiddleName,
	GenderCode AS i_GenderCode,
	Birthdate AS i_Birthdate,
	addr_line_1 AS i_addr_line_1,
	zip_postal_code AS i_zip_postal_code,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditId,
	SYSDATE AS o_CreatedDate,
	i_LicenseState AS o_LicenseState,
	i_LicenseNumber AS o_LicenseNumber,
	i_LastName AS o_LastName,
	-- *INF*: SUBSTR(i_FirstName, 1, 20)
	SUBSTR(i_FirstName, 1, 20) AS o_FirstName,
	i_MiddleName AS o_MiddleName,
	-- *INF*: TO_CHAR(TRUNC(i_Birthdate, 'DD'),'YYYYMMDD')
	TO_CHAR(CAST(TRUNC(i_Birthdate, 'DAY') AS TIMESTAMP_NTZ(0)), 'YYYYMMDD') AS o_Birthdate,
	i_zip_postal_code AS o_ZipCode,
	i_addr_line_1 AS o_StreetAddress,
	i_GenderCode AS o_Gender,
	i_pol_num||i_pol_mod AS o_PolicyNumber,
	-- *INF*: TO_CHAR(TRUNC(i_pol_exp_date, 'DD'),'YYYYMMDD')
	TO_CHAR(CAST(TRUNC(i_pol_exp_date, 'DAY') AS TIMESTAMP_NTZ(0)), 'YYYYMMDD') AS o_PolicyExpirationDate,
	i_pol_num||i_pol_mod AS o_QuotebackPolicyNumber,
	i_AgencyCode AS o_QuotebackAgencyNumber,
	i_LicenseNumber AS o_QuotebackDriverLicense,
	i_LicenseState AS o_QuotebackState,
	'   ' AS o_InsuranceIndicator,
	'100000000' AS o_ProductFlags,
	'611853' AS o_AccountNumber
	FROM SQ_Driver
),
ExploreEARSDriverRecord AS (
	TRUNCATE TABLE @{pipeline().parameters.TARGET_TABLE_OWNER}.ExploreEARSDriverRecord;
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.ExploreEARSDriverRecord
	(AuditId, CreatedDate, LicenseState, LicenseNumber, LastName, FirstName, MiddleName, Birthdate, ZipCode, StreetAddress, Gender, PolicyNumber, PolicyExpirationDate, QuotebackPolicyNumber, QuotebackAgencyNumber, QuotebackDriverLicense, QuotebackState, InsuranceIndicator, ProductFlags, AccountNumber)
	SELECT 
	o_AuditId AS AUDITID, 
	o_CreatedDate AS CREATEDDATE, 
	o_LicenseState AS LICENSESTATE, 
	o_LicenseNumber AS LICENSENUMBER, 
	o_LastName AS LASTNAME, 
	o_FirstName AS FIRSTNAME, 
	o_MiddleName AS MIDDLENAME, 
	o_Birthdate AS BIRTHDATE, 
	o_ZipCode AS ZIPCODE, 
	o_StreetAddress AS STREETADDRESS, 
	o_Gender AS GENDER, 
	o_PolicyNumber AS POLICYNUMBER, 
	o_PolicyExpirationDate AS POLICYEXPIRATIONDATE, 
	o_QuotebackPolicyNumber AS QUOTEBACKPOLICYNUMBER, 
	o_QuotebackAgencyNumber AS QUOTEBACKAGENCYNUMBER, 
	o_QuotebackDriverLicense AS QUOTEBACKDRIVERLICENSE, 
	o_QuotebackState AS QUOTEBACKSTATE, 
	o_InsuranceIndicator AS INSURANCEINDICATOR, 
	o_ProductFlags AS PRODUCTFLAGS, 
	o_AccountNumber AS ACCOUNTNUMBER
	FROM EXP_Calculate
),