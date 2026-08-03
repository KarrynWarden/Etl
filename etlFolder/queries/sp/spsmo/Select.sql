select code,name,dbegin,dend,longname,idrw
,successor,egrulname,bosssn,bossfn,bosspn,buhsn,buhfn,buhpn,bosslong, dogbosspost, signmain, docn, docdt,bossfamdog, bossimdog, bossotdog, bosspost
from 
(
select s.code,nvl(s.namehead,s.name) name,s.dbegin,s.dend,s.idrw
,row_number() over (partition by s.code order by s.dend desc) rw
,s1.headsmo successor
,s.longname,s.name egrulname,s.bosssn,s.bossfn,s.bosspn,s.buhsn,s.buhfn,s.buhpn,s.bosslong, s.dogbosspost, s.signmain, s.docn, s.docdt,s.bossfamdog, s.bossimdog, s.bossotdog, s.bosspost
from spsmo s
left join spsmo s1 on s.code = s1.code and s1.code<>s1.headsmo
where s.code = s.headsmo
      and (s1.dbegin is null or s1.dbegin = (select min(dbegin) from spsmo where code = s.code and code<>headsmo))
)
