SELECT
uid,
date,
device,
hitnum,
hit,
referral,
source,
mdm,
keyword,
landpg,
goalpg,
car.car_no as carno,
car.maker_name as maker_name,
car.car_name as car_name

FROM [kishita_risu.ga_GAID]
JOIN EACH [weekly_upload.Cardata]  as car
ON ordernum = car.order_num
;
