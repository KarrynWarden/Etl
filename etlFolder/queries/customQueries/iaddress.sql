-- КЛАУД: пометка №2 для проверки правила «ваши файлы не переписываются»
select IDROW, TYPEADDRESS, SUBJ, INDX, OKATO, RNNAME, NPNAME, UL, DOM, KORP, KV, DREG
, FIAS_AOID, FIAS_HOUSEID, IPRESENTER_IDRW, GAR_AOGUID, GAR_HSGUID
, IDRW, CREATEDATE, TEXT ----новые поля
from public.iaddress a