select pers.IDROW
---,null TYPEROW,null IDEDROW
,ID,NPOLIS::numeric,VPOLIS,ENP::numeric,FAM,IM,OT,W,DR,DBEG,DEND,DVIZIT,DENDDOC,REASON,SMO,PRZCOD,EXT,TERR,C_OKSM,BOMG,SS 
,doc.DOCTYPE DOCTYPE,doc.DOCSER DOCSER, doc.DOCNUM DOCNUM, doc.DOCDATE DOCDATE, doc.name_vp name_vp
,MR,method
---, null PETITION
, RSMO,RPOLIS,FPOLIS
---, null NORDER, null DORDER
,PHONE, EMAIL
,FIOPR,CONTACT
,TRUE_DR,NUMBLANK
---,null UNWORK,null DENDTEMP
,META_FAM,META_IM,META_OT
---,null LEADZERO
,DOSTDRCAL,BIRTH_OKSM,KATEG,DGET,DBegERZ,DEndERZ,IDMAIN,DDeath
---,null DFPOLIS
,DBegArmy,date_trunc('second',lastupdate) lastupdate
---,null FVS,null STFERZL
, createdate
from public.iperson pers
left join (select distinct on (idrow) idrow,doctype,docser,docnum,docdate,name_vp
from idoc d,public.spdocper sd 
where typerow = 1 and sd.code = d.doctype ----and d.idrow = 159264
order by idrow,priority,docdate desc, source
 ) doc on doc.idrow = pers.idrow