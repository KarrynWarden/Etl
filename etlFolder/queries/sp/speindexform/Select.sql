SELECT code, "name", "period",
case 
when ffinsource = '0' then 0
when ffinsource = '1,2,3,4' then 1
when ffinsource = '1' then 2
when ffinsource = '2,3,4' then 3
end as ffinsource, 
fplan, fstru, dbegin, dend, numbform, typeform, '', idrw from speindexform
