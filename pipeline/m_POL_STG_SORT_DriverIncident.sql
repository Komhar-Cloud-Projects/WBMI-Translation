WITH
SQ_DriverIncidentFile AS (

-- TODO Manual --

),
SRTTRANS AS (
	SELECT
	PolNumber, 
	DriverState, 
	DriverLicense, 
	LastName, 
	FirstName, 
	MiddleName, 
	IncidentStart, 
	IncidentEnd AS IncicentEnd, 
	IncidentDesc, 
	IncidentPoints, 
	IncidentCode, 
	UnderwriterLastName, 
	UnderwriterFirstName
	FROM SQ_DriverIncidentFile
	ORDER BY UnderwriterLastName ASC, UnderwriterFirstName ASC
),
DriverIncidentFile1 AS (
	INSERT INTO DriverIncidentFile
	(PolNumber, DriverState, DriverLicense, LastName, FirstName, MiddleName, IncidentStart, IncicentEnd, IncidentDesc, IncidentPoints, IncidentCode, UnderwriterFirstName, UnderwriterLastName)
	SELECT 
	POLNUMBER, 
	DRIVERSTATE, 
	DRIVERLICENSE, 
	LASTNAME, 
	FIRSTNAME, 
	MIDDLENAME, 
	INCIDENTSTART, 
	INCICENTEND, 
	INCIDENTDESC, 
	INCIDENTPOINTS, 
	INCIDENTCODE, 
	UNDERWRITERFIRSTNAME, 
	UNDERWRITERLASTNAME
	FROM SRTTRANS
),