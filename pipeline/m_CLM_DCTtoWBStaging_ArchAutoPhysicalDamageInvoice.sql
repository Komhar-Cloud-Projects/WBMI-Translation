WITH
SQ_AutoPhysicalDamageInvoice AS (
	-- CreatedDate from <= previous year and BillMonth is NULL
	select i1.AutoPhysicalDamageInvoiceId,
		i1.CreatedDate,
		i1.ModifiedDate,
		i1.ProcessStatus,
		i1.ProcessedEFT,
		i1.InvoiceType,
		i1.InvoiceNumber,
		i1.BillDate,
		i1.ClaimNumber,
		i1.ValReq,
		i1.VIN,
		i1.PolicyNumber,
		i1.VehicleOwner,
		i1.AppraiserName,
		i1.AdjusterName,
		i1.LossType,
		i1.LossDate,
		i1.LossState,
		i1.BillItemDescription,
		i1.BilledAmount,
		i1.Tax,
		i1.TotalAmount,
		i1.PaidByDraftKey,
		LEFT(i1.ErrorDescription, 8000) AS ErrorDescription,
		i1.BillMonth 
	FROM AutoPhysicalDamageInvoice i1
	WHERE (DATEDIFF(yyyy, i1.CreatedDate, getDate()) >= @{pipeline().parameters.NUM_YEARS} and i1.BillMonth is NULL)
	 @{pipeline().parameters.WHERE_CLAUSE} 
	UNION ALL
	-- All rows for BillMonths that have MAX(CreatedDate) from <= previous year, and no incomplete rows (i.e. ProcessStatus in ('P','F','A'))
	select i2.AutoPhysicalDamageInvoiceId,
		i2.CreatedDate,
		i2.ModifiedDate,
		i2.ProcessStatus,
		i2.ProcessedEFT,
		i2.InvoiceType,
		i2.InvoiceNumber,
		i2.BillDate,
		i2.ClaimNumber,
		i2.ValReq,
		i2.VIN,
		i2.PolicyNumber,
		i2.VehicleOwner,
		i2.AppraiserName,
		i2.AdjusterName,
		i2.LossType,
		i2.LossDate,
		i2.LossState,
		i2.BillItemDescription,
		i2.BilledAmount,
		i2.Tax,
		i2.TotalAmount,
		i2.PaidByDraftKey,
		LEFT(i2.ErrorDescription, 8000) AS ErrorDescription,
		i2.BillMonth 
	FROM AutoPhysicalDamageInvoice i2
	WHERE i2.BillMonth in (
		select candidateMonth.BillMonth
		from AutoPhysicalDamageInvoice candidateMonth
		where candidateMonth.BillMonth is not NULL
			and not exists (select 1
				from AutoPhysicalDamageInvoice incompleteMonth
				where incompleteMonth.BillMonth = candidateMonth.BillMonth
					and incompleteMonth.ProcessStatus in ('P','F','A'))
		group by candidateMonth.BillMonth
		having DATEDIFF(yyyy, MAX(candidateMonth.CreatedDate), getDate()) >= @{pipeline().parameters.NUM_YEARS}
		)
	 @{pipeline().parameters.WHERE_CLAUSE}
),
EXP_AutoPhysicalDamageInvoice AS (
	SELECT
	AutoPhysicalDamageInvoiceId,
	CreatedDate,
	ModifiedDate,
	ProcessStatus,
	ProcessedEFT,
	InvoiceType,
	InvoiceNumber,
	BillDate,
	ClaimNumber,
	ValReq,
	VIN,
	PolicyNumber,
	VehicleOwner,
	AppraiserName,
	AdjusterName,
	LossType,
	LossDate,
	LossState,
	BillItemDescription,
	BilledAmount,
	Tax,
	TotalAmount,
	PaidByDraftKey,
	ErrorDescription,
	BillMonth
	FROM SQ_AutoPhysicalDamageInvoice
),
ArchAutoPhysicalDamageInvoice AS (
	INSERT INTO ArchAutoPhysicalDamageInvoice
	(AutoPhysicalDamageInvoiceId, CreatedDate, ModifiedDate, ProcessStatus, ProcessedEFT, InvoiceType, InvoiceNumber, BillDate, ClaimNumber, ValReq, VIN, PolicyNumber, VehicleOwner, AppraiserName, AdjusterName, LossType, LossDate, LossState, BillItemDescription, BilledAmount, Tax, TotalAmount, PaidByDraftKey, ErrorDescription, BillMonth)
	SELECT 
	AUTOPHYSICALDAMAGEINVOICEID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	PROCESSSTATUS, 
	PROCESSEDEFT, 
	INVOICETYPE, 
	INVOICENUMBER, 
	BILLDATE, 
	CLAIMNUMBER, 
	VALREQ, 
	VIN, 
	POLICYNUMBER, 
	VEHICLEOWNER, 
	APPRAISERNAME, 
	ADJUSTERNAME, 
	LOSSTYPE, 
	LOSSDATE, 
	LOSSSTATE, 
	BILLITEMDESCRIPTION, 
	BILLEDAMOUNT, 
	TAX, 
	TOTALAMOUNT, 
	PAIDBYDRAFTKEY, 
	ERRORDESCRIPTION, 
	BILLMONTH
	FROM EXP_AutoPhysicalDamageInvoice
),
SQ_AutoPhysicalDamageInvoice_Delete AS (
	-- CreatedDate from <=last year and BillMonth is NULL
	select i1.AutoPhysicalDamageInvoiceId 
	FROM AutoPhysicalDamageInvoice i1
	WHERE (DATEDIFF(yyyy, i1.CreatedDate, getDate()) >= @{pipeline().parameters.NUM_YEARS} and i1.BillMonth is NULL)
	 @{pipeline().parameters.WHERE_CLAUSE} 
	UNION ALL
	-- All rows for BillMonths that have MAX(CreatedDate) from <= previous year, and no incomplete rows (i.e. ProcessStatus in ('P','F','A'))
	select i2.AutoPhysicalDamageInvoiceId 
	FROM AutoPhysicalDamageInvoice i2
	WHERE i2.BillMonth in (
		select candidateMonth.BillMonth
		from AutoPhysicalDamageInvoice candidateMonth
		where candidateMonth.BillMonth is not NULL
			and not exists (select 1
				from AutoPhysicalDamageInvoice incompleteMonth
				where incompleteMonth.BillMonth = candidateMonth.BillMonth
					and incompleteMonth.ProcessStatus in ('P','F','A'))
		group by candidateMonth.BillMonth
		having DATEDIFF(yyyy, MAX(candidateMonth.CreatedDate), getDate()) >= @{pipeline().parameters.NUM_YEARS}
		)
	 @{pipeline().parameters.WHERE_CLAUSE}
),
EXPTRANS AS (
	SELECT
	AutoPhysicalDamageInvoiceId
	FROM SQ_AutoPhysicalDamageInvoice_Delete
),
UPDTRANS AS (
	SELECT
	AutoPhysicalDamageInvoiceId
	FROM EXPTRANS
),
AutoPhysicalDamageInvoice_Delete AS (
	DELETE FROM AutoPhysicalDamageInvoice
	WHERE (AutoPhysicalDamageInvoiceId) IN (SELECT  AUTOPHYSICALDAMAGEINVOICEID FROM UPDTRANS)
),