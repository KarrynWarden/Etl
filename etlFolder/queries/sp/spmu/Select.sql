select code,name,terr,typework,dbegin,dend,longname,idrw,inn,kpp
,successor,egrulname,properform,mlevel,chiefsn,chieffn,chiefpn,buhsn,buhfn,buhpn,chiefsndog,chieffndog,chiefpndog
from 
(
select m.code,m.name,nvl(m.egrulname,m.shortname) shortname,m.dbegin,m.dend,m.idrow idrw
,decode(mm.successor,0,null,mm.successor) successor
,m.terr,m.typework,m.longname,a.inn,a.kpp,m.egrulname,m.properform,m.mlevel,m.chiefsn,m.chieffn,m.chiefpn,m.buhsn,m.buhfn,m.buhpn,m.chiefsndog,m.chieffndog,m.chiefpndog
,row_number() over (partition by m.code order by m.dend desc) rw
from spmu m
left join spmumain mm on mm.code = m.code
left join advicemo a on a.mo = m.code and least(m.dend,trunc(sysdate)) between to_date(a.advyear||'-01-01', 'YYYY-MM-DD') and to_date(a.advyear||'-12-31', 'YYYY-MM-DD') and a.advtype = 1
)