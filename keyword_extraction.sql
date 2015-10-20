SELECT
c_keyword,
COUNT(c_keyword) AS count
FROM [kishita_risu_Sep.ga_0901_0930_all]
WHERE
REGEXP_MATCH(c_keyword,r".*(not provided).*") <> true
AND
REGEXP_MATCH(c_keyword,r".*ガリバー.*") <> true
AND
REGEXP_MATCH(c_keyword,r"Gulliver") <> true
AND
REGEXP_MATCH(c_keyword,r"gulliver") <> true
AND
REGEXP_MATCH(c_keyword,r"車") <> true
AND
REGEXP_MATCH(c_keyword,r"中古車") <> true
AND
REGEXP_MATCH(c_keyword,r"ガリバー.*中古車") <> true
AND
REGEXP_MATCH(c_keyword,r"中古車.*ガリバー") <> true
AND
REGEXP_MATCH(c_keyword,r".*がりば.*") <> true
AND
REGEXP_MATCH(c_keyword,r"中古軽自動車") <> true
AND
REGEXP_MATCH(c_keyword,r"燃費ランキング") <> true
AND
REGEXP_MATCH(c_keyword,r"軽自動車 中古") <> true
AND
REGEXP_MATCH(c_keyword,r"軽自動車中古") <> true
AND
REGEXP_MATCH(c_keyword,r"gullivar 車販売する") <> true
AND
REGEXP_MATCH(c_keyword,r"gariba-") <> true
AND
REGEXP_MATCH(c_keyword,r".*カタログ.*") <> true
AND
REGEXP_MATCH(c_keyword,r"未使用車") <> true
GROUP BY c_keyword
ORDER BY count DESC
LIMIT 300
;
