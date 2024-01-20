WITH
SQ_WorkDCTPLLocation AS (
	SELECT
		WorkDCTPLLocationId,
		ExtractDate,
		SourceSystemId,
		PolicyId,
		PolicyKey,
		AddressId,
		AddressKey,
		PolicyNumber,
		PolicySymbol,
		PolicyVersion,
		InsuredObjectNumber,
		StreetAddressLine1,
		StateUspsCode,
		CityName,
		CountyName,
		TerritoryCode,
		PostalCode,
		StartDate
	FROM WorkDCTPLLocation
),
EXP_SRC_DataCollect AS (
	SELECT
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_Auditid,
	WorkDCTPLLocationId,
	ExtractDate,
	SourceSystemId,
	PolicyId,
	PolicyKey,
	AddressId,
	AddressKey,
	PolicyNumber,
	PolicySymbol,
	PolicyVersion,
	InsuredObjectNumber,
	StreetAddressLine1,
	StateUspsCode,
	CityName,
	CountyName,
	TerritoryCode,
	PostalCode,
	StartDate
	FROM SQ_WorkDCTPLLocation
),
ArchWorkDCTPLLocation AS (
	INSERT INTO ArchWorkDCTPLLocation
	(Auditid, ExtractDate, SourceSystemId, WorkDCTPLLocationId, PolicyId, PolicyKey, AddressId, AddressKey, PolicyNumber, PolicySymbol, PolicyVersion, InsuredObjectNumber, StreetAddressLine1, StateUspsCode, CityName, CountyName, TerritoryCode, PostalCode, StartDate)
	SELECT 
	o_Auditid AS AUDITID, 
	EXTRACTDATE, 
	SOURCESYSTEMID, 
	WORKDCTPLLOCATIONID, 
	POLICYID, 
	POLICYKEY, 
	ADDRESSID, 
	ADDRESSKEY, 
	POLICYNUMBER, 
	POLICYSYMBOL, 
	POLICYVERSION, 
	INSUREDOBJECTNUMBER, 
	STREETADDRESSLINE1, 
	STATEUSPSCODE, 
	CITYNAME, 
	COUNTYNAME, 
	TERRITORYCODE, 
	POSTALCODE, 
	STARTDATE
	FROM EXP_SRC_DataCollect
),