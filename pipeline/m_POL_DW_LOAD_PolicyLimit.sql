WITH
LKP_SRC_Dept1553_D_AND_O AS (
	SELECT
	Value,
	in_PifSymbol,
	in_PifPolicyNumber,
	in_PifModule,
	in_TYPE,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	TYPE
	FROM (
		SELECT 
		ltrim(rtrim(replace(replace(replace(replace(replace(replace(
		case 
		when DECLPTFormNumber in ('CPNFP01','CPNFP02','WBDOA01') and charindex('(A) EACH LOSS',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('(A) EACH LOSS',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('(A) EACH LOSS',SourceValue)))-CHARINDEX('@',SourceValue,charindex('(A) EACH LOSS',SourceValue))-1)
		     when DECLPTFormNumber in ('CPNFP01','CPNFP02','WBDOA01') and charindex('(B) EACH POLICY YEAR',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('(B) EACH POLICY YEAR',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('(B) EACH POLICY YEAR',SourceValue)))-CHARINDEX('@',SourceValue,charindex('(B) EACH POLICY YEAR',SourceValue))-1)
		     when DECLPTFormNumber in ('CPEPL02', 'CPEPL03') and charindex('LIMIT OF LIABILITY:  EACH CLAIM LIMIT',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM LIMIT',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM LIMIT',SourceValue))-1)
			 when DECLPTFormNumber in ('CPEPL02', 'CPEPL03') and charindex('POLICY AGGREGATE LIMIT',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue))-1)
			 when DECLPTFormNumber in ('CPNFE01', 'NSDOA01') and charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue))-1)
			 when DECLPTFormNumber in ('CPNFE01', 'NSDOA01') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))-1)	 
			when DECLPTFormNumber in ('CPNFP01','CPNFP02') and charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue))-1)
			 when DECLPTFormNumber in ('CPNFP01','CPNFP02') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))-1)	 
		
			when DECLPTFormNumber in ('CPNFP01','CPNFP02','WBDOA01') and charindex('(A) EACH LOSS',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('(A) EACH LOSS',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('(A) EACH LOSS',SourceValue)))-CHARINDEX('@',SourceValue,charindex('(A) EACH LOSS',SourceValue))-1)
		     when DECLPTFormNumber in ('CPNFP01','CPNFP02','WBDOA01') and charindex('(B) EACH POLICY YEAR',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('(B) EACH POLICY YEAR',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('(B) EACH POLICY YEAR',SourceValue)))-CHARINDEX('@',SourceValue,charindex('(B) EACH POLICY YEAR',SourceValue))-1)
		     when DECLPTFormNumber in ('CPEPL02', 'CPEPL03') and charindex('LIMIT OF LIABILITY:  EACH CLAIM LIMIT',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM LIMIT',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM LIMIT',SourceValue))-1)
			 when DECLPTFormNumber in ('CPEPL02', 'CPEPL03') and charindex('POLICY AGGREGATE LIMIT',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue))-1)
			 when DECLPTFormNumber in ('CPNFE01', 'NSDOA01') and charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue))-1)
			 when DECLPTFormNumber in ('CPNFE01', 'NSDOA01') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))-1)	 
			when DECLPTFormNumber in ('CPNFP01','CPNFP02') and charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue))-1)
			 when DECLPTFormNumber in ('CPNFP01','CPNFP02') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))-1)
			 else 'N/A'
		end,',',''),' ',''),'.',''),'O','0'),'\',''),'$','')))	  AS VALUE,
		PifSymbol as PifSymbol,
		PifPolicyNumber as PifPolicyNumber,
		PifModule as PifModule,
		case when DECLPTFormNumber in ('CPNFP01','CPNFP02','WBDOA01') and charindex('(A) EACH LOSS',SourceValue )>0				then 'PolicyPerClaimLimit'     
			 when DECLPTFormNumber in ('CPNFP01','CPNFP02','WBDOA01') and charindex('(B) EACH POLICY YEAR',SourceValue )>0		then 'PolicyAggregateLimit'     
			 when DECLPTFormNumber in ('CPEPL02', 'CPEPL03') and charindex('LIMIT OF LIABILITY:  EACH CLAIM LIMIT',SourceValue )>0 then 'PolicyPerClaimLimit'     
			 when DECLPTFormNumber in ('CPEPL02', 'CPEPL03') and charindex('POLICY AGGREGATE LIMIT',SourceValue )>0				then 'PolicyAggregateLimit'
			 when DECLPTFormNumber in ('CPNFE01', 'NSDOA01') and charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue )>0				then 'PolicyPerClaimLimit'     
		     when DECLPTFormNumber in ('CPNFE01', 'NSDOA01') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0		then 'PolicyAggregateLimit'     
		     when DECLPTFormNumber in ('CPNFP01','CPNFP02') and charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue )>0		then 'PolicyPerClaimLimit'     
			 when DECLPTFormNumber in ('CPNFP01','CPNFP02') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0				then 'PolicyAggregateLimit' 
		     ELSE 'N/A'end as TYPE
		
		from (
		select PifSymbol,
		PifPolicyNumber,
		PifModule,
		ltrim(rtrim(DECLPTFormNumber)) as DECLPTFormNumber,
		ltrim(rtrim(DECLPTText1701))+' '+ltrim(rtrim(DECLPTText71791)) as SourceValue
		from DBO.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553} dept1553
		where 
		DECLPTFormNumber in ('CPNFP01','CPNFP02','WBDOA01','CPEPL02', 'CPEPL03','CPNFE01','NSDOA01','CPNFP01','CPNFP02')
		and not exists (
		select 1
		from DBO.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553}
		where dept1553.PifSymbol=PifSymbol
		and dept1553.PifPolicyNumber=PifPolicyNumber
		and dept1553.PifModule=PifModule
		and RIGHT(dept1553.DECLPTFormNumber,2)=RIGHT(DECLPTFormNumber,2)
		and dept1553.DECLPTSeq0098=DECLPTSeq0098
		and dept1553.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553ID}<@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553}.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553ID})
		union all
		select PifSymbol,
		PifPolicyNumber,
		PifModule,
		ltrim(rtrim(DECLPTFormNumber)) as DECLPTFormNumber,
		ltrim(rtrim(DECLPTText1702))+' '+ltrim(rtrim(DECLPTText71792)) as SourceValue
		from DBO.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553} dept1553
		where 
		DECLPTFormNumber in ('CPNFP01','CPNFP02','WBDOA01','CPEPL02', 'CPEPL03','CPNFE01','NSDOA01','CPNFP01','CPNFP02')
		and not exists (
		select 1
		from DBO.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553}
		where dept1553.PifSymbol=PifSymbol
		and dept1553.PifPolicyNumber=PifPolicyNumber
		and dept1553.PifModule=PifModule
		and RIGHT(dept1553.DECLPTFormNumber,2)=RIGHT(DECLPTFormNumber,2)
		and dept1553.DECLPTSeq0098=DECLPTSeq0098
		and dept1553.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553ID}<@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553}.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553ID})
		) Common
		where  
		(DECLPTFormNumber in ('CPNFP01','CPNFP02','WBDOA01') and charindex('(A) EACH LOSS',SourceValue )>0				)or    
			(DECLPTFormNumber in ('CPNFP01','CPNFP02','WBDOA01') and charindex('(B) EACH POLICY YEAR',SourceValue )>0		)or     
			(DECLPTFormNumber in ('CPEPL02', 'CPEPL03') and charindex('LIMIT OF LIABILITY:  EACH CLAIM LIMIT',SourceValue )>0)or
			(DECLPTFormNumber in ('CPEPL02', 'CPEPL03') and charindex('POLICY AGGREGATE LIMIT',SourceValue )>0				)or
			(DECLPTFormNumber in ('CPNFE01', 'NSDOA01') and charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue )>0				)or    
		    (DECLPTFormNumber in ('CPNFE01', 'NSDOA01') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0		)or     
		    (DECLPTFormNumber in ('CPNFP01','CPNFP02') and charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue )>0		)or    
			(DECLPTFormNumber in ('CPNFP01','CPNFP02') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0				)
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,TYPE ORDER BY Value DESC) = 1
),
LKP_SRC_Dept1553_EPLI AS (
	SELECT
	Value,
	in_PifSymbol,
	in_PifPolicyNumber,
	in_PifModule,
	in_TYPE,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	TYPE
	FROM (
		SELECT
		ltrim(rtrim(replace(replace(replace(replace(replace(replace(
		case when DECLPTFormNumber in ('CPEPL01') and charindex('LIMIT OF LIABILITY:  EACH RELATED WRONGFUL EMPLOYMENT PRACTICE',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH RELATED WRONGFUL EMPLOYMENT PRACTICE',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH RELATED WRONGFUL EMPLOYMENT PRACTICE',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH RELATED WRONGFUL EMPLOYMENT PRACTICE',SourceValue))-1)
			 when DECLPTFormNumber in ('CPNFP02') and charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue))-1)
			 when DECLPTFormNumber in ('NSDOA01') and charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue))-1)
			 when DECLPTFormNumber in ('CPEPL01','CPNFP02','NSDOA01') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))-1)
		     when DECLPTFormNumber in ('CPEPL02','CPEPL03','CPEPL04') and charindex('EACH CLAIM LIMIT',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('EACH CLAIM LIMIT',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('EACH CLAIM LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('EACH CLAIM LIMIT',SourceValue))-1)
			 when DECLPTFormNumber in ('CPEPL02','CPEPL03','CPEPL04') and charindex('POLICY AGGREGATE LIMIT',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue))-1)
			 when DECLPTFormNumber in ('SLMYM01') and charindex('SELF-INSURED RETENTION',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('SELF-INSURED RETENTION',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('SELF-INSURED RETENTION',SourceValue)))-CHARINDEX('@',SourceValue,charindex('SELF-INSURED RETENTION',SourceValue))-1)
			 when DECLPTFormNumber in ('SLMYM01') and charindex('LIMIT OF LIABILITY',SourceValue )>0 AND CHARINDEX('',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY',SourceValue))+1,
		          CHARINDEX('',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY',SourceValue))-1) 
		
			when DECLPTFormNumber in ('CPEPL01') and charindex('LIMIT OF LIABILITY:  EACH RELATED WRONGFUL EMPLOYMENT PRACTICE',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH RELATED WRONGFUL EMPLOYMENT PRACTICE',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH RELATED WRONGFUL EMPLOYMENT PRACTICE',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH RELATED WRONGFUL EMPLOYMENT PRACTICE',SourceValue))-1)
			 when DECLPTFormNumber in ('CPNFP02') and charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue))-1)
			 when DECLPTFormNumber in ('NSDOA01') and charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue))-1)
			 when DECLPTFormNumber in ('CPEPL01','CPNFP02','NSDOA01') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('TOTAL AGGREGATE LIMIT',SourceValue))-1)
		     when DECLPTFormNumber in ('CPEPL02','CPEPL03','CPEPL04') and charindex('EACH CLAIM LIMIT',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('EACH CLAIM LIMIT',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('EACH CLAIM LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('EACH CLAIM LIMIT',SourceValue))-1)
			 when DECLPTFormNumber in ('CPEPL02','CPEPL03','CPEPL04') and charindex('POLICY AGGREGATE LIMIT',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue)))-CHARINDEX('@',SourceValue,charindex('POLICY AGGREGATE LIMIT',SourceValue))-1)
			 when DECLPTFormNumber in ('SLMYM01') and charindex('SELF-INSURED RETENTION',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('SELF-INSURED RETENTION',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('SELF-INSURED RETENTION',SourceValue)))-CHARINDEX('@',SourceValue,charindex('SELF-INSURED RETENTION',SourceValue))-1)
			 when DECLPTFormNumber in ('SLMYM01') and charindex('LIMIT OF LIABILITY',SourceValue )>0 AND CHARINDEX('^',SourceValue)>0
		     then SUBSTRING(SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY',SourceValue))+1,
		          CHARINDEX('^',SourceValue,CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY',SourceValue)))-CHARINDEX('@',SourceValue,charindex('LIMIT OF LIABILITY',SourceValue))-1)
			 else 'N/A'
		end,',',''),' ',''),'.',''),'O','0'),'\',''),'$','')))	  AS VALUE,
		PifSymbol as PifSymbol,
		PifPolicyNumber as PifPolicyNumber,
		PifModule as PifModule,
		case when DECLPTFormNumber in ('CPEPL01') and charindex('LIMIT OF LIABILITY:  EACH RELATED WRONGFUL EMPLOYMENT PRACTICE',SourceValue )>0				then 'PolicyPerClaimLimit'  
		     when DECLPTFormNumber in ('CPNFP02') and charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue )>0				then 'PolicyPerClaimLimit'  
		     when DECLPTFormNumber in ('NSDOA01') and charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue )>0				then 'PolicyPerClaimLimit'       
		     when DECLPTFormNumber in ('CPEPL01','CPNFP02','NSDOA01') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0		then 'PolicyAggregateLimit'        
			 when DECLPTFormNumber in ('CPEPL02','CPEPL03','CPEPL04') and charindex('EACH CLAIM LIMIT',SourceValue )>0				then 'PolicyPerClaimLimit'     
		     when DECLPTFormNumber in ('CPEPL02','CPEPL03','CPEPL04') and charindex('POLICY AGGREGATE LIMIT',SourceValue )>0		then 'PolicyAggregateLimit'     
			 when DECLPTFormNumber in ('SLMYM01') and charindex('SELF-INSURED RETENTION',SourceValue )>0				then 'PolicyPerClaimLimit'       
			 when DECLPTFormNumber in ('SLMYM01') and charindex('LIMIT OF LIABILITY',SourceValue )>0				then 'PolicyAggregateLimit'     
		     ELSE 'N/A'end as TYPE
		from (
		select PifSymbol,
		PifPolicyNumber,
		PifModule,
		LTRIM(RTRIM(DECLPTFormNumber)) as DECLPTFormNumber,
		ltrim(rtrim(DECLPTText1701))+' '+ltrim(rtrim(DECLPTText71791)) as SourceValue
		from DBO.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553} dept1553
		where 
		DECLPTFormNumber in ('CPEPL01','CPEPL02','CPEPL03','CPEPL04','CPNFP02','NSDOA01','SLMYM01')
		and not exists (
		select 1
		from DBO.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553}
		where dept1553.PifSymbol=PifSymbol
		and dept1553.PifPolicyNumber=PifPolicyNumber
		and dept1553.PifModule=PifModule
		and RIGHT(dept1553.DECLPTFormNumber,2)=RIGHT(DECLPTFormNumber,2)
		and dept1553.DECLPTSeq0098=DECLPTSeq0098
		and dept1553.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553ID}<@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553}.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553ID})
		union all
		select PifSymbol,
		PifPolicyNumber,
		PifModule,
		LTRIM(RTRIM(DECLPTFormNumber)) as DECLPTFormNumber,
		ltrim(rtrim(DECLPTText1702))+' '+ltrim(rtrim(DECLPTText71792)) as SourceValue
		from DBO.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553} dept1553
		where 
		DECLPTFormNumber in ('CPEPL01','CPEPL02','CPEPL03','CPEPL04','CPNFP02','NSDOA01','SLMYM01')
		and not exists (
		select 1
		from DBO.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553}
		where dept1553.PifSymbol=PifSymbol
		and dept1553.PifPolicyNumber=PifPolicyNumber
		and dept1553.PifModule=PifModule
		and RIGHT(dept1553.DECLPTFormNumber,2)=RIGHT(DECLPTFormNumber,2)
		and dept1553.DECLPTSeq0098=DECLPTSeq0098
		and dept1553.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553ID}<@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553}.@{pipeline().parameters.SOURCE_TABLE_NAME_DEPT1553ID})
		
		) Common
		where 
		(DECLPTFormNumber in ('CPEPL01') and charindex('LIMIT OF LIABILITY:  EACH RELATED WRONGFUL EMPLOYMENT PRACTICE',SourceValue )>0	) or			 
		(DECLPTFormNumber in ('CPNFP02') and charindex('LIMIT OF LIABILITY:  EACH LOSS',SourceValue )>0				                          ) or
		(DECLPTFormNumber in ('NSDOA01') and charindex('LIMIT OF LIABILITY:  EACH CLAIM',SourceValue )>0				                        ) or
		(DECLPTFormNumber in ('CPEPL01','CPNFP02','NSDOA01') and charindex('TOTAL AGGREGATE LIMIT',SourceValue )>0		                  ) or
		(DECLPTFormNumber in ('CPEPL02','CPEPL03','CPEPL04') and charindex('EACH CLAIM LIMIT',SourceValue )>0				                    ) or
		(DECLPTFormNumber in ('CPEPL02','CPEPL03','CPEPL04') and charindex('POLICY AGGREGATE LIMIT',SourceValue )>0		                  ) or
		(DECLPTFormNumber in ('SLMYM01') and charindex('SELF-INSURED RETENTION',SourceValue )>0				                                  ) or
		(DECLPTFormNumber in ('SLMYM01') and charindex('LIMIT OF LIABILITY',SourceValue )>0				                                      )
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,TYPE ORDER BY Value DESC) = 1
),
LKP_Pif11Stage_WB100 AS (
	SELECT
	LimitValue,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	LimitType
	FROM (
		select 
		PifSymbol as PifSymbol,
		PifPolicyNumber as PifPolicyNumber,
		PifModule as PifModule,
		LimitType as LimitType,
		LimitValue as LimitValue
		from (
		select PifSymbol as PifSymbol,
		PifPolicyNumber as PifPolicyNumber,
		PifModule as PifModule,
		LTRIM(RTRIM(replace(raw_type,'0',''))) as LimitType,
		case when patindex('% [0-9]%',raw_value)>0 then ltrim(rtrim(substring(raw_value,1,patindex('% [0-9]%',raw_value)))) when patindex('% [A-Z]%',raw_value)>0 then ltrim(rtrim(substring(raw_value,1,patindex('% [A-Z]%',raw_value)))) else raw_value end as LimitValue,
		row_number() over(partition by PifSymbol, PifPolicyNumber, PifModule,raw_type order by DocumentSegmentSeq desc,id desc) as rn
		from (
		select 
		id as id,
		DocumentSegmentSeq as DocumentSegmentSeq,
		PifSymbol as PifSymbol,
		PifPolicyNumber as PifPolicyNumber,
		PifModule as PifModule,
		ltrim(rtrim(case when id=1 then raw_type_1 else raw_type_2 end)) as raw_type,
		ltrim(rtrim(replace(replace(case when id=1 then raw_value_1 else raw_value_2 end,'$',''),',',''))) as raw_value
		from (
		select 
		t1.DocumentSegmentSeq as DocumentSegmentSeq
		,t1.PifSymbol as PifSymbol
		,t1.PifPolicyNumber as PifPolicyNumber
		,t1.PifModule as PifModule
		,t1.DocumentText as DocumentText
		,replace(substring(t1.DocumentText,patindex('%LIMIT  %$%',t1.DocumentText)-17,22),'ITY:','') as raw_type_1
		,substring(substring(t1.DocumentText,patindex('%LIMIT  %$%',t1.DocumentText)+5,50),patindex('%$%',substring(t1.DocumentText,patindex('%LIMIT  %$%',t1.DocumentText)+5,50)),22) as raw_value_1
		,case when t1.DocumentText like '%LIMIT  %$%LIMIT  %$%' then replace(substring(
		  substring(t1.DocumentText,patindex('%LIMIT  %$%',t1.DocumentText)+5,len(t1.DocumentText)),
		  patindex('%LIMIT  %$%',substring(t1.DocumentText,patindex('%LIMIT  %$%',t1.DocumentText)+5,len(t1.DocumentText)))-17
		  ,22
		  ),'ITY:','')
		else NULL end as raw_type_2
		,case when t1.DocumentText like '%LIMIT  %$%LIMIT  %$%' then substring(
		  substring(t1.DocumentText,patindex('%LIMIT  %$%',t1.DocumentText)+5,len(t1.DocumentText)),
		  patindex('%LIMIT  %$%',substring(t1.DocumentText,patindex('%LIMIT  %$%',t1.DocumentText)+5,len(t1.DocumentText)))+5
		  ,30)
		else NULL end as raw_value_2
		from dbo.@{pipeline().parameters.SOURCE_TABLE_NAME_PIF11} t1
		where t1.DocumentType='CM'and t1.DocumentName in ('WB100','WB100L')
		and not exists(
		  select 1 from dbo.@{pipeline().parameters.SOURCE_TABLE_NAME_PIF11} t2
		  where t1.DocumentType=t2.DocumentType
		  and t1.DocumentName=t2.DocumentName
		  and t1.PifSymbol=t2.PifSymbol
		  and t1.PifPolicyNumber=t2.PifPolicyNumber
		  and t1.PifModule=t2.PifModule
		  and t1.DocumentSegmentSeq=t2.DocumentSegmentSeq
		  and t1.@{pipeline().parameters.SOURCE_COL_NAME_PIFF11STAGEID}<t2.@{pipeline().parameters.SOURCE_COL_NAME_PIFF11STAGEID}
		)
		and patindex('%LIMIT  %$%',t1.DocumentText)>0) raw_limit
		join (
		select 1 as id
		union all
		select 2 as id
		) dummy_table
		on 1=1
		) pivot_limit
		where raw_type is not null
		and raw_value is not null) a
		where rn=1
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,LimitType ORDER BY LimitValue) = 1
),
SQ_pif_4514_stage AS (
	SELECT DISTINCT RTRIM(A.pif_symbol) as pif_symbol,
	       A.pif_policy_number as pif_policy_number,
	       A.pif_module as pif_module,
	       ltrim(rtrim(sar_insurance_line)) as sar_insurance_line, 
	case when ltrim(rtrim(sar_risk_unit_group)) in ('286','287','900','367','366')
				then ltrim(rtrim(sar_risk_unit_group)) ELSE '' END as sar_risk_unit_group
	FROM  @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_4514}  A
	WHERE ((sar_insurance_line='GL' AND sar_major_peril<>'517') OR sar_insurance_line='WC')
	
	@{pipeline().parameters.WHERE_CLAUSE}
),
AGG_RemoveDuplicates AS (
	SELECT
	pif_symbol,
	pif_policy_number,
	pif_module,
	sar_insurance_line,
	sar_risk_unit_group AS i_sar_risk_unit_group,
	-- *INF*: MAX(i_sar_risk_unit_group)
	MAX(i_sar_risk_unit_group
	) AS o_sar_risk_unit_group
	FROM SQ_pif_4514_stage
	GROUP BY pif_symbol, pif_policy_number, pif_module, sar_insurance_line
),
EXP_Set_Values AS (
	SELECT
	sar_insurance_line AS i_sar_insurance_line,
	pif_symbol,
	pif_policy_number,
	pif_module,
	pif_symbol||pif_policy_number||pif_module AS o_pol_key,
	o_sar_risk_unit_group AS sar_risk_unit_group,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(i_sar_insurance_line)
	:UDF.DEFAULT_VALUE_FOR_STRINGS(i_sar_insurance_line
	) AS o_InsuranceLine
	FROM AGG_RemoveDuplicates
),
LKP_Pif43LXZWCStage AS (
	SELECT
	Pmdl4w1CovIiLmtsStdEach,
	Pmdl4w1CovIiLmtsStdPol,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdl4w1InsuranceLine
	FROM (
		SELECT PifSymbol as PifSymbol
			,PifPolicyNumber as PifPolicyNumber
			,PifModule as PifModule
			,Pmdl4w1InsuranceLine as Pmdl4w1InsuranceLine
			,Pmdl4w1CovIiLmtsStdEach as Pmdl4w1CovIiLmtsStdEach
			,Pmdl4w1CovIiLmtsStdPol as Pmdl4w1CovIiLmtsStdPol
		FROM @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_43LXZWC}
		WHERE Pmdl4w1InsuranceLine='WC' and (Pmdl4w1CovIiLmtsStdEach is not null or Pmdl4w1CovIiLmtsStdPol is not null)
		@{pipeline().parameters.ORDER_CLAUSE_43LXZWC}
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmdl4w1InsuranceLine ORDER BY Pmdl4w1CovIiLmtsStdEach) = 1
),
LKP_Pif43RXGLStage_Agg AS (
	SELECT
	Pmdrxg1LimOcc,
	Pmdrxg1LimitSubline,
	Pmdrxg1RiskTypeInd,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdrxg1InsuranceLine
	FROM (
		SELECT pmdrxg1limocc                       AS Pmdrxg1LimOcc, 
				  Pmdrxg1LimitSubline     AS Pmdrxg1LimitSubline,
		Pmdrxg1RiskTypeInd as Pmdrxg1RiskTypeInd,
		               pifsymbol                           AS PifSymbol, 
		               pifpolicynumber                     AS PifPolicyNumber, 
		               pifmodule                           AS PifModule, 
		               Ltrim(Rtrim(pmdrxg1insuranceline))  AS Pmdrxg1InsuranceLine
		FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_43RXGL} 
		 WHERE  pmdrxg1insuranceline = 'GL' 
		AND (Pmdrxg1RiskTypeInd='O' AND Pmdrxg1PmsDefGlSubline='340' OR Pmdrxg1PmsDefGlSubline IN ('345','346','355','365','367'))
		AND ( ( pmdrxg1limocc IS NOT NULL 
		 AND Ltrim(Rtrim(pmdrxg1limocc)) <> '' ) 
		OR (Pmdrxg1LimitSubline IS NOT NULL
		AND LTRIM(RTRIM(Pmdrxg1LimitSubline))<>''))
		@{pipeline().parameters.ORDER_CLAUSE_43RXGL}
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmdrxg1InsuranceLine ORDER BY Pmdrxg1LimOcc) = 1
),
LKP_Pif43RXGLStage_ProductAgg AS (
	SELECT
	Pmdrxg1LimOcc,
	Pmdrxg1LimitSubline,
	PifSymbol,
	PifPolicyNumber,
	PifModule,
	Pmdrxg1InsuranceLine
	FROM (
		SELECT pmdrxg1limocc                       AS Pmdrxg1LimOcc, 
				  Pmdrxg1LimitSubline     AS Pmdrxg1LimitSubline,
		               pifsymbol                           AS PifSymbol, 
		               pifpolicynumber                     AS PifPolicyNumber, 
		               pifmodule                           AS PifModule, 
		               Ltrim(Rtrim(pmdrxg1insuranceline))  AS Pmdrxg1InsuranceLine
		FROM   @{pipeline().parameters.SOURCE_TABLE_OWNER}.@{pipeline().parameters.SOURCE_TABLE_NAME_43RXGL} 
		 WHERE  pmdrxg1insuranceline = 'GL' 
		AND Pmdrxg1RiskTypeInd='P'
		AND ( ( pmdrxg1limocc IS NOT NULL 
		 AND Ltrim(Rtrim(pmdrxg1limocc)) <> '' ) 
		OR (Pmdrxg1LimitSubline IS NOT NULL
		AND LTRIM(RTRIM(Pmdrxg1LimitSubline))<>''))
		@{pipeline().parameters.ORDER_CLAUSE_43RXGL}
		--
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY PifSymbol,PifPolicyNumber,PifModule,Pmdrxg1InsuranceLine ORDER BY Pmdrxg1LimOcc) = 1
),
LKP_Policy AS (
	SELECT
	pol_ak_id,
	pol_key
	FROM (
		SELECT 
			pol_ak_id,
			pol_key
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER_V2}.policy
		WHERE crrnt_snpsht_flag=1 AND source_sys_id='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY pol_key ORDER BY pol_ak_id) = 1
),
EXP_IDAndLookupValues AS (
	SELECT
	EXP_Set_Values.pif_symbol,
	EXP_Set_Values.pif_policy_number,
	EXP_Set_Values.pif_module,
	LKP_Pif43RXGLStage_Agg.Pmdrxg1LimOcc AS i_Pmdrxg1LimOcc_Agg,
	LKP_Pif43RXGLStage_Agg.Pmdrxg1LimitSubline AS i_Pmdrxg1LimitSubline_Agg,
	LKP_Pif43RXGLStage_Agg.Pmdrxg1RiskTypeInd AS i_Pmdrxg1RiskTypeInd,
	LKP_Pif43RXGLStage_ProductAgg.Pmdrxg1LimOcc AS i_Pmdrxg1LimOcc_ProductAgg,
	LKP_Pif43RXGLStage_ProductAgg.Pmdrxg1LimitSubline AS i_Pmdrxg1LimitSubline_ProductAgg,
	LKP_Pif43LXZWCStage.Pmdl4w1CovIiLmtsStdEach AS i_Pmdl4w1CovIiLmtsStdEach,
	LKP_Pif43LXZWCStage.Pmdl4w1CovIiLmtsStdPol AS i_Pmdl4w1CovIiLmtsStdPol,
	LKP_Policy.pol_ak_id AS i_pol_ak_id,
	EXP_Set_Values.sar_risk_unit_group AS i_sar_risk_unit_group,
	EXP_Set_Values.o_InsuranceLine AS InsuranceLine,
	-- *INF*: IIF(:UDF.DEFAULT_VALUE_FOR_STRINGS(i_Pmdrxg1LimOcc_Agg)='N/A',:UDF.DEFAULT_VALUE_FOR_STRINGS(i_Pmdrxg1LimOcc_ProductAgg),:UDF.DEFAULT_VALUE_FOR_STRINGS(i_Pmdrxg1LimOcc_Agg))
	IFF(:UDF.DEFAULT_VALUE_FOR_STRINGS(i_Pmdrxg1LimOcc_Agg
		) = 'N/A',
		:UDF.DEFAULT_VALUE_FOR_STRINGS(i_Pmdrxg1LimOcc_ProductAgg
		),
		:UDF.DEFAULT_VALUE_FOR_STRINGS(i_Pmdrxg1LimOcc_Agg
		)
	) AS v_PolicyPerOccurenceLimit,
	-- *INF*: IIF(ISNULL(i_Pmdrxg1LimitSubline_Agg) OR IS_SPACES(i_Pmdrxg1LimitSubline_Agg) OR LENGTH(i_Pmdrxg1LimitSubline_Agg)=0,'N/A',LTRIM(RTRIM(i_Pmdrxg1LimitSubline_Agg)))
	IFF(i_Pmdrxg1LimitSubline_Agg IS NULL 
		OR LENGTH(i_Pmdrxg1LimitSubline_Agg)>0 AND TRIM(i_Pmdrxg1LimitSubline_Agg)='' 
		OR LENGTH(i_Pmdrxg1LimitSubline_Agg
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_Pmdrxg1LimitSubline_Agg
			)
		)
	) AS v_Pmdrxg1LimitSubline_Agg,
	-- *INF*: IIF(ISNULL(i_Pmdrxg1LimitSubline_ProductAgg) OR IS_SPACES(i_Pmdrxg1LimitSubline_ProductAgg) OR LENGTH(i_Pmdrxg1LimitSubline_ProductAgg)=0,'N/A',LTRIM(RTRIM(i_Pmdrxg1LimitSubline_ProductAgg)))
	IFF(i_Pmdrxg1LimitSubline_ProductAgg IS NULL 
		OR LENGTH(i_Pmdrxg1LimitSubline_ProductAgg)>0 AND TRIM(i_Pmdrxg1LimitSubline_ProductAgg)='' 
		OR LENGTH(i_Pmdrxg1LimitSubline_ProductAgg
		) = 0,
		'N/A',
		LTRIM(RTRIM(i_Pmdrxg1LimitSubline_ProductAgg
			)
		)
	) AS v_Pmdrxg1LimitSubline_ProductAgg,
	-- *INF*: IIF(ISNULL(i_Pmdl4w1CovIiLmtsStdEach), 'N/A', TO_CHAR(i_Pmdl4w1CovIiLmtsStdEach))
	IFF(i_Pmdl4w1CovIiLmtsStdEach IS NULL,
		'N/A',
		TO_CHAR(i_Pmdl4w1CovIiLmtsStdEach
		)
	) AS v_Pmdl4w1CovIiLmtsStdEach,
	-- *INF*: IIF(ISNULL(i_Pmdl4w1CovIiLmtsStdPol), 'N/A', TO_CHAR(i_Pmdl4w1CovIiLmtsStdPol))
	IFF(i_Pmdl4w1CovIiLmtsStdPol IS NULL,
		'N/A',
		TO_CHAR(i_Pmdl4w1CovIiLmtsStdPol
		)
	) AS v_Pmdl4w1CovIiLmtsStdPol,
	i_pol_ak_id AS o_pol_ak_id,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(IIF(InsuranceLine='GL' AND i_sar_risk_unit_group!='367',v_PolicyPerOccurenceLimit,'N/A'),'0'))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(IFF(InsuranceLine = 'GL' 
				AND i_sar_risk_unit_group != '367',
				v_PolicyPerOccurenceLimit,
				'N/A'
			), '0'
		)
	) AS o_PolicyPerOccurenceLimit,
	-- *INF*: DECODE(TRUE,
	-- InsuranceLine='WC',v_Pmdl4w1CovIiLmtsStdPol,
	-- InsuranceLine='GL' and in(i_sar_risk_unit_group,'286', '287', '900')  , 
	-- IIF(IN(:LKP.LKP_SRC_Dept1553_D_AND_O(pif_symbol,pif_policy_number,pif_module,'PolicyAggregateLimit'),'SEEWB100','SEEWB100L'),
	-- :LKP.LKP_PIF11STAGE_WB100(pif_symbol,pif_policy_number,pif_module,'POLICY AGGREGATE LIMIT'),
	-- :LKP.LKP_SRC_Dept1553_D_AND_O(pif_symbol,pif_policy_number,pif_module,'PolicyAggregateLimit')
	-- ),
	-- InsuranceLine='GL' and in(i_sar_risk_unit_group,'366')  , 
	-- IIF(IN(:LKP.LKP_SRC_Dept1553_EPLI(pif_symbol,pif_policy_number,pif_module,'PolicyAggregateLimit'),'SEEWB100','SEEWB100L'),
	-- :LKP.LKP_PIF11STAGE_WB100(pif_symbol,pif_policy_number,pif_module,'POLICY AGGREGATE LIMIT'),
	-- :LKP.LKP_SRC_Dept1553_EPLI(pif_symbol,pif_policy_number,pif_module,'PolicyAggregateLimit')
	-- ),
	-- InsuranceLine='GL' and in(i_sar_risk_unit_group,'367'), v_Pmdrxg1LimitSubline_Agg, 
	-- InsuranceLine='GL' ,v_Pmdrxg1LimitSubline_Agg,
	-- 'N/A')
	-- 
	DECODE(TRUE,
		InsuranceLine = 'WC', v_Pmdl4w1CovIiLmtsStdPol,
		InsuranceLine = 'GL' 
		AND i_sar_risk_unit_group IN ('286','287','900'), IFF(LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.Value IN ('SEEWB100','SEEWB100L'),
			LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_POLICY_AGGREGATE_LIMIT.LimitValue,
			LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.Value
		),
		InsuranceLine = 'GL' 
		AND i_sar_risk_unit_group IN ('366'), IFF(LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.Value IN ('SEEWB100','SEEWB100L'),
			LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_POLICY_AGGREGATE_LIMIT.LimitValue,
			LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.Value
		),
		InsuranceLine = 'GL' 
		AND i_sar_risk_unit_group IN ('367'), v_Pmdrxg1LimitSubline_Agg,
		InsuranceLine = 'GL', v_Pmdrxg1LimitSubline_Agg,
		'N/A'
	) AS v_PolicyAggregateLimit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(IIF(ISNULL(v_PolicyAggregateLimit), 'N/A', v_PolicyAggregateLimit ),'0'))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(IFF(v_PolicyAggregateLimit IS NULL,
				'N/A',
				v_PolicyAggregateLimit
			), '0'
		)
	) AS o_PolicyAggregateLimit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(IIF(InsuranceLine='GL' ,v_Pmdrxg1LimitSubline_ProductAgg,'N/A'),'0'))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(IFF(InsuranceLine = 'GL',
				v_Pmdrxg1LimitSubline_ProductAgg,
				'N/A'
			), '0'
		)
	) AS o_PolicyProductAggregateLimit,
	v_Pmdl4w1CovIiLmtsStdEach AS o_PolicyPerAccidentLimit,
	v_Pmdl4w1CovIiLmtsStdEach AS o_PolicyPerDiseaseLimit,
	-- *INF*: DECODE(True, 
	-- InsuranceLine='GL' and in(i_sar_risk_unit_group,'286', '287', '900') ,
	-- IIF(IN(:LKP.LKP_SRC_Dept1553_D_AND_O(pif_symbol,pif_policy_number,pif_module,'PolicyPerClaimLimit'),'SEEWB100','SEEWB100L'),
	-- :LKP.LKP_PIF11STAGE_WB100(pif_symbol,pif_policy_number,pif_module,'EACH CLAIM LIMIT'),
	-- :LKP.LKP_SRC_Dept1553_D_AND_O(pif_symbol,pif_policy_number,pif_module,'PolicyPerClaimLimit')
	-- ),
	-- InsuranceLine='GL' and in(i_sar_risk_unit_group,'367'), i_Pmdrxg1LimOcc_Agg, 
	-- InsuranceLine='GL' and in(i_sar_risk_unit_group,'366') ,
	-- IIF(IN(:LKP.LKP_SRC_Dept1553_EPLI(pif_symbol,pif_policy_number,pif_module,'PolicyPerClaimLimit'),'SEEWB100','SEEWB100L'),
	-- :LKP.LKP_PIF11STAGE_WB100(pif_symbol,pif_policy_number,pif_module,'EACH CLAIM LIMIT'),
	-- :LKP.LKP_SRC_Dept1553_EPLI(pif_symbol,pif_policy_number,pif_module,'PolicyPerClaimLimit')
	-- ),
	-- 'N/A'
	--  )
	DECODE(True,
		InsuranceLine = 'GL' 
		AND i_sar_risk_unit_group IN ('286','287','900'), IFF(LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.Value IN ('SEEWB100','SEEWB100L'),
			LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_EACH_CLAIM_LIMIT.LimitValue,
			LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.Value
		),
		InsuranceLine = 'GL' 
		AND i_sar_risk_unit_group IN ('367'), i_Pmdrxg1LimOcc_Agg,
		InsuranceLine = 'GL' 
		AND i_sar_risk_unit_group IN ('366'), IFF(LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.Value IN ('SEEWB100','SEEWB100L'),
			LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_EACH_CLAIM_LIMIT.LimitValue,
			LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.Value
		),
		'N/A'
	) AS v_PolicyPerClaimLimit,
	-- *INF*: :UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(IIF(ISNULL(v_PolicyPerClaimLimit), 'N/A', v_PolicyPerClaimLimit),'0'))
	:UDF.DEFAULT_VALUE_FOR_STRINGS(LTRIM(IFF(v_PolicyPerClaimLimit IS NULL,
				'N/A',
				v_PolicyPerClaimLimit
			), '0'
		)
	) AS o_PolicyPerClaimLimit
	FROM EXP_Set_Values
	LEFT JOIN LKP_Pif43LXZWCStage
	ON LKP_Pif43LXZWCStage.PifSymbol = EXP_Set_Values.pif_symbol AND LKP_Pif43LXZWCStage.PifPolicyNumber = EXP_Set_Values.pif_policy_number AND LKP_Pif43LXZWCStage.PifModule = EXP_Set_Values.pif_module AND LKP_Pif43LXZWCStage.Pmdl4w1InsuranceLine = EXP_Set_Values.o_InsuranceLine
	LEFT JOIN LKP_Pif43RXGLStage_Agg
	ON LKP_Pif43RXGLStage_Agg.PifSymbol = EXP_Set_Values.pif_symbol AND LKP_Pif43RXGLStage_Agg.PifPolicyNumber = EXP_Set_Values.pif_policy_number AND LKP_Pif43RXGLStage_Agg.PifModule = EXP_Set_Values.pif_module AND LKP_Pif43RXGLStage_Agg.Pmdrxg1InsuranceLine = EXP_Set_Values.o_InsuranceLine
	LEFT JOIN LKP_Pif43RXGLStage_ProductAgg
	ON LKP_Pif43RXGLStage_ProductAgg.PifSymbol = EXP_Set_Values.pif_symbol AND LKP_Pif43RXGLStage_ProductAgg.PifPolicyNumber = EXP_Set_Values.pif_policy_number AND LKP_Pif43RXGLStage_ProductAgg.PifModule = EXP_Set_Values.pif_module AND LKP_Pif43RXGLStage_ProductAgg.Pmdrxg1InsuranceLine = EXP_Set_Values.o_InsuranceLine
	LEFT JOIN LKP_Policy
	ON LKP_Policy.pol_key = EXP_Set_Values.o_pol_key
	LEFT JOIN LKP_SRC_DEPT1553_D_AND_O LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit
	ON LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.PifSymbol = pif_symbol
	AND LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.PifPolicyNumber = pif_policy_number
	AND LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.PifModule = pif_module
	AND LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.TYPE = 'PolicyAggregateLimit'

	LEFT JOIN LKP_PIF11STAGE_WB100 LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_POLICY_AGGREGATE_LIMIT
	ON LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_POLICY_AGGREGATE_LIMIT.PifSymbol = pif_symbol
	AND LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_POLICY_AGGREGATE_LIMIT.PifPolicyNumber = pif_policy_number
	AND LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_POLICY_AGGREGATE_LIMIT.PifModule = pif_module
	AND LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_POLICY_AGGREGATE_LIMIT.LimitType = 'POLICY AGGREGATE LIMIT'

	LEFT JOIN LKP_SRC_DEPT1553_EPLI LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit
	ON LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.PifSymbol = pif_symbol
	AND LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.PifPolicyNumber = pif_policy_number
	AND LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.PifModule = pif_module
	AND LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyAggregateLimit.TYPE = 'PolicyAggregateLimit'

	LEFT JOIN LKP_SRC_DEPT1553_D_AND_O LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit
	ON LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.PifSymbol = pif_symbol
	AND LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.PifPolicyNumber = pif_policy_number
	AND LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.PifModule = pif_module
	AND LKP_SRC_DEPT1553_D_AND_O_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.TYPE = 'PolicyPerClaimLimit'

	LEFT JOIN LKP_PIF11STAGE_WB100 LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_EACH_CLAIM_LIMIT
	ON LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_EACH_CLAIM_LIMIT.PifSymbol = pif_symbol
	AND LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_EACH_CLAIM_LIMIT.PifPolicyNumber = pif_policy_number
	AND LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_EACH_CLAIM_LIMIT.PifModule = pif_module
	AND LKP_PIF11STAGE_WB100_pif_symbol_pif_policy_number_pif_module_EACH_CLAIM_LIMIT.LimitType = 'EACH CLAIM LIMIT'

	LEFT JOIN LKP_SRC_DEPT1553_EPLI LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit
	ON LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.PifSymbol = pif_symbol
	AND LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.PifPolicyNumber = pif_policy_number
	AND LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.PifModule = pif_module
	AND LKP_SRC_DEPT1553_EPLI_pif_symbol_pif_policy_number_pif_module_PolicyPerClaimLimit.TYPE = 'PolicyPerClaimLimit'

),
LKP_PolicyLimit AS (
	SELECT
	PolicyLimitId,
	PolicyLimitAKId,
	PolicyPerOccurenceLimit,
	PolicyAggregateLimit,
	PolicyProductAggregateLimit,
	PolicyPerAccidentLimit,
	PolicyPerDiseaseLimit,
	PolicyPerClaimLimit,
	InsuranceLine,
	PolicyAKId
	FROM (
		SELECT 
			PolicyLimitId,
			PolicyLimitAKId,
			PolicyPerOccurenceLimit,
			PolicyAggregateLimit,
			PolicyProductAggregateLimit,
			PolicyPerAccidentLimit,
			PolicyPerDiseaseLimit,
			PolicyPerClaimLimit,
			InsuranceLine,
			PolicyAKId
		FROM @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit
		WHERE CurrentSnapshotFlag=1 AND SourceSystemID='@{pipeline().parameters.SOURCE_SYSTEM_ID}'
	)
	QUALIFY ROW_NUMBER() OVER (PARTITION BY InsuranceLine,PolicyAKId ORDER BY PolicyLimitId) = 1
),
SEQ_PolicyLimitAKId AS (
	CREATE SEQUENCE SEQ_PolicyLimitAKId
	START = 0
	INCREMENT = 1;
),
EXP_DetectChange AS (
	SELECT
	LKP_PolicyLimit.PolicyLimitId AS lkp_PolicyLimitId,
	LKP_PolicyLimit.PolicyLimitAKId AS lkp_PolicyLimitAKId,
	LKP_PolicyLimit.PolicyPerOccurenceLimit AS lkp_PolicyPerOccurenceLimit,
	LKP_PolicyLimit.PolicyAggregateLimit AS lkp_PolicyAggregateLimit,
	LKP_PolicyLimit.PolicyProductAggregateLimit AS lkp_PolicyProductAggregateLimit,
	LKP_PolicyLimit.PolicyPerAccidentLimit AS lkp_PolicyPerAccidentLimit,
	LKP_PolicyLimit.PolicyPerDiseaseLimit AS lkp_PolicyPerDiseaseLimit,
	LKP_PolicyLimit.PolicyPerClaimLimit AS lkp_PolicyPerClaimLimit,
	SEQ_PolicyLimitAKId.NEXTVAL AS i_NEXTVAL,
	EXP_IDAndLookupValues.o_pol_ak_id AS pol_ak_id,
	EXP_IDAndLookupValues.InsuranceLine,
	EXP_IDAndLookupValues.o_PolicyPerOccurenceLimit AS PolicyPerOccurenceLimit,
	EXP_IDAndLookupValues.o_PolicyAggregateLimit AS PolicyAggregateLimit,
	EXP_IDAndLookupValues.o_PolicyProductAggregateLimit AS PolicyProductAggregateLimit,
	EXP_IDAndLookupValues.o_PolicyPerAccidentLimit AS PolicyPerAccidentLimit,
	EXP_IDAndLookupValues.o_PolicyPerDiseaseLimit AS PolicyPerDiseaseLimit,
	EXP_IDAndLookupValues.o_PolicyPerClaimLimit AS PolicyPerClaimLimit,
	-- *INF*: DECODE(TRUE,ISNULL(lkp_PolicyLimitId) AND pol_ak_id<>-1,1,
	-- pol_ak_id<>-1 
	-- AND (lkp_PolicyPerOccurenceLimit<>PolicyPerOccurenceLimit
	-- OR lkp_PolicyAggregateLimit<>PolicyAggregateLimit
	-- OR lkp_PolicyProductAggregateLimit<>PolicyProductAggregateLimit
	-- OR lkp_PolicyPerAccidentLimit<>PolicyPerAccidentLimit
	-- OR lkp_PolicyPerDiseaseLimit<>PolicyPerDiseaseLimit
	-- OR lkp_PolicyPerClaimLimit<>PolicyPerClaimLimit),2,0)
	DECODE(TRUE,
		lkp_PolicyLimitId IS NULL 
		AND pol_ak_id <> - 1, 1,
		pol_ak_id <> - 1 
		AND ( lkp_PolicyPerOccurenceLimit <> PolicyPerOccurenceLimit 
			OR lkp_PolicyAggregateLimit <> PolicyAggregateLimit 
			OR lkp_PolicyProductAggregateLimit <> PolicyProductAggregateLimit 
			OR lkp_PolicyPerAccidentLimit <> PolicyPerAccidentLimit 
			OR lkp_PolicyPerDiseaseLimit <> PolicyPerDiseaseLimit 
			OR lkp_PolicyPerClaimLimit <> PolicyPerClaimLimit 
		), 2,
		0
	) AS v_change_flag,
	'1' AS o_CurrentSnapshotFlag_Active,
	'0' AS o_CurrentSnapshotFlag_Inactive,
	@{pipeline().parameters.WBMI_AUDIT_CONTROL_RUN_ID} AS o_AuditID,
	-- *INF*: IIF(v_change_flag=1,TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'),SYSDATE)
	IFF(v_change_flag = 1,
		TO_DATE('01/01/1800 00:00:00', 'MM/DD/YYYY HH24:MI:SS'
		),
		SYSDATE
	) AS o_EffectiveDate,
	-- *INF*: TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS')
	TO_DATE('12/31/2100 23:59:59', 'MM/DD/YYYY HH24:MI:SS'
	) AS o_ExpirationDate_Active,
	-- *INF*: ADD_TO_DATE(SYSDATE,'SS',-1)
	DATEADD(SECOND,- 1,SYSDATE) AS o_ExpirationDate_Inactive,
	@{pipeline().parameters.SOURCE_SYSTEM_ID} AS o_SourceSystemID,
	SYSDATE AS o_CreatedDate,
	SYSDATE AS o_ModifiedDate,
	-- *INF*: IIF(v_change_flag=1,i_NEXTVAL,lkp_PolicyLimitAKId)
	IFF(v_change_flag = 1,
		i_NEXTVAL,
		lkp_PolicyLimitAKId
	) AS o_PolicyLimitAKId,
	v_change_flag AS o_change_flag
	FROM EXP_IDAndLookupValues
	LEFT JOIN LKP_PolicyLimit
	ON LKP_PolicyLimit.InsuranceLine = EXP_IDAndLookupValues.InsuranceLine AND LKP_PolicyLimit.PolicyAKId = EXP_IDAndLookupValues.o_pol_ak_id
),
RTR_Insert_Update AS (
	SELECT
	o_change_flag AS i_change_flag,
	lkp_PolicyLimitId AS PolicyLimitId_Inactive,
	o_CurrentSnapshotFlag_Active AS CurrentSnapshotFlag_Active,
	o_CurrentSnapshotFlag_Inactive AS CurrentSnapshotFlag_Inactive,
	o_AuditID AS AuditID,
	o_EffectiveDate AS EffectiveDate,
	o_ExpirationDate_Active AS ExpirationDate_Active,
	o_ExpirationDate_Inactive AS ExpirationDate_Inactive,
	o_SourceSystemID AS SourceSystemID,
	o_CreatedDate AS CreatedDate,
	o_ModifiedDate AS ModifiedDate,
	o_PolicyLimitAKId AS PolicyLimitAKId,
	pol_ak_id,
	InsuranceLine,
	PolicyPerOccurenceLimit,
	PolicyAggregateLimit,
	PolicyProductAggregateLimit,
	PolicyPerAccidentLimit,
	PolicyPerDiseaseLimit,
	PolicyPerClaimLimit
	FROM EXP_DetectChange
),
RTR_Insert_Update_INSERT AS (SELECT * FROM RTR_Insert_Update WHERE i_change_flag=1),
RTR_Insert_Update_UPDATE AS (SELECT * FROM RTR_Insert_Update WHERE i_change_flag=2),
TGT_PolicyLimit_INSERT_NEW AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, PolicyLimitAKId, PolicyAKId, InsuranceLine, PolicyPerOccurenceLimit, PolicyAggregateLimit, PolicyProductAggregateLimit, PolicyPerAccidentLimit, PolicyPerDiseaseLimit, PolicyPerClaimLimit)
	SELECT 
	CurrentSnapshotFlag_Active AS CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	ExpirationDate_Active AS EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYLIMITAKID, 
	pol_ak_id AS POLICYAKID, 
	INSURANCELINE, 
	POLICYPEROCCURENCELIMIT, 
	POLICYAGGREGATELIMIT, 
	POLICYPRODUCTAGGREGATELIMIT, 
	POLICYPERACCIDENTLIMIT, 
	POLICYPERDISEASELIMIT, 
	POLICYPERCLAIMLIMIT
	FROM RTR_Insert_Update_INSERT
),
UPD_PolicyLimit AS (
	SELECT
	PolicyLimitId_Inactive, 
	CurrentSnapshotFlag_Inactive, 
	ExpirationDate_Inactive, 
	ModifiedDate
	FROM RTR_Insert_Update_UPDATE
),
TGT_PolicyLimit_EXPIRE AS (
	MERGE INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit AS T
	USING UPD_PolicyLimit AS S
	ON T.PolicyLimitId = S.PolicyLimitId_Inactive
	WHEN MATCHED BY TARGET THEN
	UPDATE SET T.CurrentSnapshotFlag = S.CurrentSnapshotFlag_Inactive, T.ExpirationDate = S.ExpirationDate_Inactive, T.ModifiedDate = S.ModifiedDate
),
TGT_PolicyLimit_UPD_INSERT AS (
	INSERT INTO @{pipeline().parameters.TARGET_TABLE_OWNER}.PolicyLimit
	(CurrentSnapshotFlag, AuditID, EffectiveDate, ExpirationDate, SourceSystemID, CreatedDate, ModifiedDate, PolicyLimitAKId, PolicyAKId, InsuranceLine, PolicyPerOccurenceLimit, PolicyAggregateLimit, PolicyProductAggregateLimit, PolicyPerAccidentLimit, PolicyPerDiseaseLimit, PolicyPerClaimLimit)
	SELECT 
	CurrentSnapshotFlag_Active AS CURRENTSNAPSHOTFLAG, 
	AUDITID, 
	EFFECTIVEDATE, 
	ExpirationDate_Active AS EXPIRATIONDATE, 
	SOURCESYSTEMID, 
	CREATEDDATE, 
	MODIFIEDDATE, 
	POLICYLIMITAKID, 
	pol_ak_id AS POLICYAKID, 
	INSURANCELINE, 
	POLICYPEROCCURENCELIMIT, 
	POLICYAGGREGATELIMIT, 
	POLICYPRODUCTAGGREGATELIMIT, 
	POLICYPERACCIDENTLIMIT, 
	POLICYPERDISEASELIMIT, 
	POLICYPERCLAIMLIMIT
	FROM RTR_Insert_Update_UPDATE
),