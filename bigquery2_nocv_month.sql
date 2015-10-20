SELECT c.value, c.uid, c.date, c.device, c.hitnum, c.hit, c.referral, c.source, c.mdm, c.keyword, c.landpg, c.goalpg
FROM
  (SELECT
  a.user_session_id as uid,
  a.date as date,
  a.hit as hit,
  a.landing_page as goalpg,
  a.device as device,
  b.val as value,
  b.referral as referral,
  b.source as source,
  b.medium as mdm,
  b.keyword as keyword,
  b.hit_number as hitnum,
  b.landing_page as landpg
FROM
  (SELECT STRING(fullVisitorId) + '-' + STRING(visitId) as user_session_id, date, hits.hitNumber as hit, 
    hits.transaction.transactionId AS transaction_id, hits.page.pagePath AS landing_page, hits.customDimensions.index AS customi, device.deviceCategory AS device
  FROM
FLATTEN(
(SELECT fullVisitorId, visitId, date, hits.hitNumber, hits.transaction.transactionId, hits.page.pagePath, hits.customDimensions.index, device.deviceCategory 
  FROM TABLE_DATE_RANGE([95479039.ga_sessions_], TIMESTAMP('2015-08-01'), TIMESTAMP('2015-08-31'))) , hits.transaction.transactionId)
WHERE
hits.customDimensions.index = 1
-- AND
-- hits.hitNumber <> 1
-- AND
--   (
--   hits.page.pagePath = "221616.com/search/proposals/overform.html" #1

--   OR hits.page.pagePath = "221616.com/kaitori/info/over.html" #2
--   OR hits.page.pagePath = "221616.com/minicle/assessment/complete/index.html"
--   OR hits.page.pagePath = "221616.com/liberala/assessment/comp/index.html"

--   OR hits.page.pagePath = "221616.com/search/inquiry/over.html" #3
--   OR hits.page.pagePath = "221616.com/minicle/stock/complete/index.html"
--   OR hits.page.pagePath = "221616.com/liberala/stock/comp/index.html"

--   OR hits.page.pagePath = "221616.com/stock_search/inquiry/over.html" #4

--   OR hits.page.pagePath = "221616.com/search/proposals/confirm.html" #5

--   OR hits.page.pagePath = "221616.com/assessment/inquiry/over.html" #6

--   OR hits.page.pagePath = "221616.com/shop/reserve/satei/over.html" #7
--   OR hits.page.pagePath = "221616.com/minicle/shop/complete/index.html"
--   OR hits.page.pagePath = "221616.com/liberala/access/comp-teian/index.html"

--   OR hits.page.pagePath = "221616.com/shop/reserve/proposal/over.html" #8
--   )
)
AS a
JOIN EACH
  (SELECT STRING(fullVisitorId) + '-' + STRING(visitId) as user_session_id, trafficSource.referralPath AS referral, trafficSource.source AS source, trafficSource.keyword AS keyword, hits.customDimensions.value AS val, 
    trafficSource.medium AS medium, hits.page.pagePath AS landing_page, hits.hitNumber AS hit_number
  FROM
FLATTEN(
(SELECT fullVisitorId, visitId, hits.hitNumber, trafficSource.referralPath, trafficSource.medium, trafficSource.source, trafficSource.keyword, hits.customDimensions.value, hits.page.pagePath 
  FROM TABLE_DATE_RANGE([95479039.ga_sessions_], TIMESTAMP('2015-08-01'), TIMESTAMP('2015-08-31'))) ,hits.hitNumber)
WHERE
hits.hitNumber = 1
AND 
trafficSource.medium = "organic"
AND
(
  REGEXP_MATCH(hits.page.pagePath,"221616.com/search/")
  -- OR REGEXP_MATCH(hits.page.pagePath,"221616.com/kw/")
  -- OR REGEXP_MATCH(hits.page.pagePath,"221616.com/stock_search/")
  )
AND
REGEXP_MATCH(hits.page.pagePath,"221616.com/search/list") <> true
AND
REGEXP_MATCH(hits.page.pagePath,"221616.com/search/inquiry/over.html") <> true
AND
REGEXP_MATCH(hits.page.pagePath,"221616.com/search/proposals/confirm.html") <> true
AND
REGEXP_MATCH(hits.page.pagePath,"221616.com/search/favoriteList/index.html") <> true
AND
REGEXP_MATCH(hits.page.pagePath,"221616.com/search/index.html") <> true
AND
REGEXP_MATCH(hits.page.pagePath,"221616.com/search/select/index.html") <> true
AND
REGEXP_MATCH(hits.page.pagePath,"221616.com/search/proposals/overform.html") <> true
AND
REGEXP_MATCH(hits.page.pagePath,"221616.com/search/stock_inquiry_mail/index.html") <> true
) 
AS b
ON a.user_session_id = b.user_session_id 
-- WHERE
-- a.hit > b.hit_number
) as c
;
