SELECT
ga.c_uid as uid,
ga.c_date as date,
ga.c_device as device,
ga.c_hitnum as hitnum,
ga.c_hit as hit,
ga.c_referral as referral,
ga.c_source as source,
ga.c_mdm as mdm,
ga.c_keyword as keyword,
ga.c_landpg as landpg,
ga.c_goalpg as goalpg,
gaid.ORDER_NUM as ordernum                                                                                                                                                                                                           
FROM [kishita_risu.ga_0101_1001] as ga
JOIN EACH [weekly_upload.GAID] as gaid
ON ga.c_value = gaid.GAID
;
