Truncate table marketing_workspace.aff_mom_cohort;
Insert into marketing_workspace.aff_mom_cohort

SELECT  
'WKDA'  as company,
date_part(d, ce.attribution_date) :: int AS first_daynumber,
to_char(date(ce.attribution_date), 'Day') AS first_weekday,
date_part(d,uq.first_appointment) :: int AS app_daynumber,
to_char(date(uq.first_appointment), 'Day') AS app_weekday,
date_part(d,first_book_datetime) :: int AS first_booking_daynumber,
to_char(date(uq.first_book_datetime), 'Day') AS first_booking_weekday,
date_part(d,uq.unsubscribed_datetime) :: int  AS unsubscribed_daynumber,
to_char(date(uq.unsubscribed_datetime), 'Day') AS unsubscribed_weekday,
TO_CHAR(ce.attribution_date, 'YYYY-MM') AS first_month,
ce.attribution_date AS first_month_day,
TO_CHAR(ce.attribution_date, 'YYYY-IW') AS first_month_week,
TO_CHAR(uq.first_appointment, 'YYYY-MM') AS app_month,
date(uq.first_appointment) AS app_month_day,
TO_CHAR(uq.first_appointment, 'YYYY-IW') AS app_month_week,
TO_CHAR(uq.first_book_datetime, 'YYYY-MM') first_booking_month,
date(uq.first_book_datetime) first_booking_day,
TO_CHAR(uq.first_book_datetime, 'YYYY-IW') first_booking_week,
TO_CHAR(uq.unsubscribed_datetime, 'YYYY-MM') unsubscribed_month,
date(uq.unsubscribed_datetime) unsubscribed_day,
TO_CHAR(uq.unsubscribed_datetime, 'YYYY-IW') unsubscribed_week,

ce.network_channel, 
ce.cc as country,
ce.join_cid,
att.partner,
att.placement,
att.admedia,
att.cluster,
0 as costs,
  sum(case when first_appointment is null then 0 else 1 end) AS appointments,
  nvl(count(connecting_lead),0) AS leads,
  sum(CASE WHEN date(first_book_datetime) = attribution_date THEN uq.direct_booking ELSE 0 END) direct_bookings,
  count(has_show) AS shows,
  count(unsubscribed_datetime) AS unsubscribtions
FROM dwh_tx.tx_cid_hist AS ce
JOIN marketing_workspace.tx_cust AS uq ON uq.connecting_lead = ce.lead_id and ce.source = uq.source
JOIN marketing_workspace.aff_translation_table AS att ON att.cid = ce.join_cid AND ce.attribution_date >= att.start_date AND ce.attribution_date <= att.end_date
left join marketing_workspace.partner_blacklist AS pbl on pbl.network_channel = ce.network_channel and pbl.partner_id = split_part(ce.join_cid,'_',3) and ce.attribution_date between pbl.start_date and pbl.end_date and pbl.country = ce.cc

WHERE ce.attribution_date >= '2014-01-01'
AND nvl(uq.first_appointment,'2014-01-01') >= '2014-01-01'
AND ce.event = 'LEAD'
AND ce.source = 1
AND uq.source = 1
AND ce.channel in ('AFF','ZAN')
AND ce.attribution_date < getdate()
AND (DATE(uq.first_book_datetime) < getdate() OR uq.first_book_datetime IS NULL)
and pbl.id is null 
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28;







Truncate table marketing_workspace.aff_mom_final_lead;
Insert into marketing_workspace.aff_mom_final_lead
Select
company,
country,
first_daynumber,
first_month,
network_channel,
join_cid as cid,
partner,
placement,
admedia,
cluster,
nvl(sum(leads),0) as leads
from marketing_workspace.aff_mom_cohort
Group by 1,2,3,4,5,6,7,8,9,10;







Truncate table marketing_workspace.aff_mom_final_bookings;
Insert into marketing_workspace.aff_mom_final_bookings
Select
company,
country,
first_booking_daynumber,
first_booking_month,
network_channel,
join_cid as cid,
partner,
placement,
admedia,
cluster,
nvl(sum(appointments),0) as bookings
from marketing_workspace.aff_mom_cohort
WHERE first_booking_month = first_month
Group by 1,2,3,4,5,6,7,8,9,10;


Truncate table marketing_workspace.aff_mom_final_appointments_shows;
Insert into marketing_workspace.aff_mom_final_appointments_shows
Select
company,
country,
app_daynumber,
app_month,
network_channel,
join_cid as cid,
partner,
placement,
admedia,
cluster,
nvl(sum(appointments),0) as appointments,
nvl(sum(shows),0) as shows
from marketing_workspace.aff_mom_cohort
WHERE app_month = first_month
Group by 1,2,3,4,5,6,7,8,9,10;

Truncate table marketing_workspace.aff_mom_final_unsubscriber;
Insert into marketing_workspace.aff_mom_final_unsubscriber
Select
company,
country,
unsubscribed_daynumber,
unsubscribed_month,
network_channel,
join_cid as cid,
partner,
placement,
admedia,
cluster,
nvl(sum(unsubscribtions),0) as unsubscribtions
from marketing_workspace.aff_mom_cohort
WHERE unsubscribed_month = first_month
Group by 1,2,3,4,5,6,7,8,9,10;


Truncate table marketing_workspace.aff_mom_final_tbl;
insert into marketing_workspace.aff_mom_final_tbl
select
a.company as company,
a.country as country,
a.first_daynumber,
a.first_month,
a.network_channel as network_channel,
a.cid,
a.partner as partner,
a.placement as placement,
a.admedia as admedia,
a.cluster as cluster,
a.leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
0 as costs
From marketing_workspace.aff_mom_final_lead as a;

Insert into marketing_workspace.aff_mom_final_tbl
select
a.company as company,
a.country as country,
a.first_booking_daynumber,
a.first_booking_month,
a.network_channel as network_channel,
a.cid,
a.partner as partner,
a.placement as placement,
a.admedia as admedia,
a.cluster as cluster,
0 as leads,
nvl(a.bookings,0) as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
0 as costs
From marketing_workspace.aff_mom_final_bookings as a;

Insert into marketing_workspace.aff_mom_final_tbl
select
a.company as company,
a.country as country,
a.app_daynumber,
a.app_month,
a.network_channel as network_channel,
a.cid,
a.partner as partner,
a.placement as placement,
a.admedia as admedia,
a.cluster as cluster,
0 as leads,
0 as bookings,
nvl(a.appointments,0) as appointments,
nvl(a.shows,0) as shows,
0 as unsubscribtions,
0 as costs
From marketing_workspace.aff_mom_final_appointments_shows as a;

Insert into marketing_workspace.aff_mom_final_tbl
select
a.company as company,
a.country as country,
a.unsubscribed_daynumber,
a.unsubscribed_month,
a.network_channel as network_channel,
a.cid,
a.partner as partner,
a.placement as placement,
a.admedia as admedia,
a.cluster as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
nvl(a.unsubscribtions,0) as unsubscribtions,
0 as costs
From marketing_workspace.aff_mom_final_unsubscriber as a;

Insert into marketing_workspace.aff_mom_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
date_part(d, a.d) :: int  as first_daynumber,
TO_CHAR(a.d, 'YYYY-MM')  as first_month,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff as a
group by 1,2,3,4,5,6,7,8;

Insert into marketing_workspace.aff_mom_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
date_part(d, a.d) :: int as first_daynumber,
TO_CHAR(a.d, 'YYYY-MM') as first_month,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
null as leads,
null as bookings,
null as appointments,
null as shows,
null as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff_at as a
group by 1,2,3,4,5,6,7,8;

Insert into marketing_workspace.aff_mom_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
date_part(d, a.d) :: int as first_daynumber,
TO_CHAR(a.d, 'YYYY-MM') as first_month,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff_es as a
group by 1,2,3,4,5,6,7,8;

Insert into marketing_workspace.aff_mom_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
date_part(d, a.d) :: int as first_daynumber,
TO_CHAR(a.d, 'YYYY-MM') as first_month,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
null as leads,
null as bookings,
null as appointments,
null as shows,
null as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff_fr as a
group by 1,2,3,4,5,6,7,8;

Insert into marketing_workspace.aff_mom_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
date_part(d, a.d) :: int as first_daynumber,
TO_CHAR(a.d, 'YYYY-MM') as first_month,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff_IT as a
group by 1,2,3,4,5,6,7,8;

Insert into marketing_workspace.aff_mom_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
date_part(d, a.d) :: int as first_daynumber,
TO_CHAR(a.d, 'YYYY-MM') as first_month,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff_nl as a
group by 1,2,3,4,5,6,7,8;

Delete from marketing_workspace.aff_mom_final_tbl Where first_month = '1999-01';
Delete from marketing_workspace.aff_mom_final_tbl Where country = '0';

Truncate table marketing_workspace.aff_wow_final_lead;
Insert into marketing_workspace.aff_wow_final_lead
Select
company,
country,
first_weekday,
first_month_week,
network_channel,
join_cid as cid,
partner,
placement,
admedia,
cluster,
nvl(sum(leads),0) as leads
from marketing_workspace.aff_mom_cohort
Group by 1,2,3,4,5,6,7,8,9,10;

Truncate table marketing_workspace.aff_wow_final_bookings;
Insert into marketing_workspace.aff_wow_final_bookings
Select
company,
country,
first_booking_weekday,
first_booking_week,
network_channel,
join_cid as cid,
partner,
placement,
admedia,
cluster,
nvl(sum(appointments),0) as bookings
from marketing_workspace.aff_mom_cohort
WHERE first_booking_week = first_month_week
Group by 1,2,3,4,5,6,7,8,9,10;


Truncate table marketing_workspace.aff_wow_final_appointments_shows;
Insert into marketing_workspace.aff_wow_final_appointments_shows
Select
company,
country,
app_weekday,
app_month_week,
network_channel,
join_cid as cid,
partner,
placement,
admedia,
cluster,
nvl(sum(appointments),0) as appointments,
nvl(sum(shows),0) as shows
from marketing_workspace.aff_mom_cohort
WHERE app_month_week = first_month_week
Group by 1,2,3,4,5,6,7,8,9,10;

Truncate table marketing_workspace.aff_wow_final_unsubscriber;
Insert into marketing_workspace.aff_wow_final_unsubscriber
Select
company,
country,
unsubscribed_weekday,
unsubscribed_week,
network_channel,
join_cid as cid,
partner,
placement,
admedia,
cluster,
nvl(sum(unsubscribtions),0) as unsubscribtions
from marketing_workspace.aff_mom_cohort
WHERE unsubscribed_week = first_month_week
Group by 1,2,3,4,5,6,7,8,9,10;



Truncate table marketing_workspace.aff_wow_final_tbl;
Insert into marketing_workspace.aff_wow_final_tbl
select
a.company as company,
a.country as country,
a.first_weekday,
a.first_month_week,
a.network_channel as network_channel,
a.cid,
a.partner as partner,
a.placement as placement,
a.admedia as admedia,
a.cluster as cluster,
a.leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
0 as costs
From marketing_workspace.aff_wow_final_lead as a;

Insert into marketing_workspace.aff_wow_final_tbl
select
a.company as company,
a.country as country,
a.first_booking_weekday as first_weekday,
a.first_booking_week as first_month_week,
a.network_channel as network_channel,
a.cid,
a.partner as partner,
a.placement as placement,
a.admedia as admedia,
a.cluster as cluster,
0 as leads,
nvl(a.bookings,0) as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
0 as costs
From marketing_workspace.aff_wow_final_bookings as a;

Insert into marketing_workspace.aff_wow_final_tbl
select
a.company as company,
a.country as country,
a.app_weekday as first_weekday,
a.app_month_week as first_month_week,
a.network_channel as network_channel,
a.cid,
a.partner as partner,
a.placement as placement,
a.admedia as admedia,
a.cluster as cluster,
0 as leads,
0 as bookings,
nvl(a.appointments,0) as appointments,
nvl(a.shows,0) as shows,
0 as unsubscribtions,
0 as costs
From marketing_workspace.aff_wow_final_appointments_shows as a;

Insert into marketing_workspace.aff_wow_final_tbl
select
a.company as company,
a.country as country,
a.unsubscribed_weekday as first_weekday,
a.unsubscribed_week as first_month_week,
a.network_channel as network_channel,
a.cid,
a.partner as partner,
a.placement as placement,
a.admedia as admedia,
a.cluster as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
nvl(a.unsubscribtions,0) as unsubscribtions,
0 as costs
From marketing_workspace.aff_wow_final_unsubscriber as a;

Insert into marketing_workspace.aff_wow_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
to_char(date(a.d), 'Day') as first_weekday,
TO_CHAR(a.d, 'YYYY-IW')  as first_month_week,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff as a
group by 1,2,3,4,5,6,7,8;

Insert into marketing_workspace.aff_wow_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
to_char(date(a.d), 'Day') as first_weekday,
TO_CHAR(a.d, 'YYYY-IW')  as first_month_week,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff_at as a
group by 1,2,3,4,5,6,7,8;

Insert into marketing_workspace.aff_wow_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
to_char(date(a.d), 'Day') as first_weekday,
TO_CHAR(a.d, 'YYYY-IW')  as first_month_week,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff_es as a
group by 1,2,3,4,5,6,7,8;

Insert into marketing_workspace.aff_wow_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
to_char(date(a.d), 'Day') as first_weekday,
TO_CHAR(a.d, 'YYYY-IW')  as first_month_week,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff_fr as a
group by 1,2,3,4,5,6,7,8;

Insert into marketing_workspace.aff_wow_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
to_char(date(a.d), 'Day') as first_weekday,
TO_CHAR(a.d, 'YYYY-IW')  as first_month_week,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff_IT as a
group by 1,2,3,4,5,6,7,8;

Insert into marketing_workspace.aff_wow_final_tbl
select
'WKDA' as company,
substring(a.partner_ID,1,2) as country,
to_char(date(a.d), 'Day') as first_weekday,
TO_CHAR(a.d, 'YYYY-IW')  as first_month_week,
a.channel as network_channel,
a.partner_ID as cid,
a.partner as partner,
a.placement as placement,
'no cluster' as admedia,
'no cluster' as cluster,
0 as leads,
0 as bookings,
0 as appointments,
0 as shows,
0 as unsubscribtions,
sum(a.cost) as costs
From marketing_workspace.virt_aff_nl as a
group by 1,2,3,4,5,6,7,8;

Delete from marketing_workspace.aff_wow_final_tbl Where first_month_week = '1999-01';
Delete from marketing_workspace.aff_wow_final_tbl Where country = '0';




