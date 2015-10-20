#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse

from apiclient.discovery import build
from apiclient.errors import HttpError
from oauth2client.client import GoogleCredentials

import json
import csv

def main(project_id):
    # [START build_service]
    # Grab the application's default credentials from the environment.
    credentials = GoogleCredentials.get_application_default()
    # Construct the service object for interacting with the BigQuery API.
    bigquery_service = build('bigquery', 'v2', credentials=credentials)
    # [END build_service]

    try:
        # [START run_query]
        query_request = bigquery_service.jobs()
        query_data = {
            'query': (
                # 'SELECT c_keyword, '
                # 'COUNT(c_keyword) AS count '
                # 'FROM [kishita_risu_Sep.ga_0901_0930_all] '
                # 'WHERE '
                # 'REGEXP_MATCH(c_keyword,r".*(not provided).*") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r".*ガリバー.*") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"Gulliver") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"gulliver") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"車") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"中古車") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"ガリバー.*中古車") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"中古車.*ガリバー") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r".*がりば.*") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"中古軽自動車") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"燃費ランキング") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"軽自動車 中古") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"軽自動車中古") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"gullivar 車販売する") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"gariba-") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r".*カタログ.*") <> true '
                # 'AND '
                # 'REGEXP_MATCH(c_keyword,r"未使用車") <> true '
                # 'GROUP BY c_keyword '
                # 'ORDER BY count DESC '
                # 'LIMIT 1000; '

                'SELECT c.value, c.uid, c.date, c.device, c.hitnum, c.hit, c.referral, c.source, c.mdm, c.keyword, c.landpg, c.goalpg '
                'FROM '
                '(SELECT '
                'a.user_session_id as uid, '
                'a.date as date, '
                'a.hit as hit, '
                'a.landing_page as goalpg, '
                'a.device as device, '
                'b.val as value, '
                'b.referral as referral, '
                'b.source as source, '
                'b.medium as mdm, '
                'b.keyword as keyword, '
                'b.hit_number as hitnum, '
                'b.landing_page as landpg '
                'FROM '
                '(SELECT STRING(fullVisitorId) + \'-\' + STRING(visitId) as user_session_id, date, hits.hitNumber as hit,  '
                'hits.transaction.transactionId AS transaction_id, hits.page.pagePath AS landing_page, hits.customDimensions.index AS customi, device.deviceCategory AS device '
                'FROM '
                'FLATTEN( '
                '(SELECT fullVisitorId, visitId, date, hits.hitNumber, hits.transaction.transactionId, hits.page.pagePath, hits.customDimensions.index, device.deviceCategory  '
                'FROM TABLE_DATE_RANGE([95479039.ga_sessions_], TIMESTAMP(\'2015-01-01\'), TIMESTAMP(\'2015-10-01\'))) , hits.transaction.transactionId) '
                'WHERE '
                'hits.customDimensions.index = 1 '
                'AND '
                '( '
                'hits.page.pagePath = "221616.com/search/proposals/overform.html" '
                'OR hits.page.pagePath = "221616.com/kaitori/info/over.html" '
                'OR hits.page.pagePath = "221616.com/minicle/assessment/complete/index.html" '
                'OR hits.page.pagePath = "221616.com/liberala/assessment/comp/index.html" '
                'OR hits.page.pagePath = "221616.com/search/inquiry/over.html" '
                'OR hits.page.pagePath = "221616.com/minicle/stock/complete/index.html" '
                'OR hits.page.pagePath = "221616.com/liberala/stock/comp/index.html" '
                'OR hits.page.pagePath = "221616.com/stock_search/inquiry/over.html" '
                'OR hits.page.pagePath = "221616.com/search/proposals/confirm.html" '
                'OR hits.page.pagePath = "221616.com/assessment/inquiry/over.html" '
                'OR hits.page.pagePath = "221616.com/shop/reserve/satei/over.html" '
                'OR hits.page.pagePath = "221616.com/minicle/shop/complete/index.html" '
                'OR hits.page.pagePath = "221616.com/liberala/access/comp-teian/index.html" '
                'OR hits.page.pagePath = "221616.com/shop/reserve/proposal/over.html" '
                ') '
                ') '
                'AS a '
                'JOIN EACH '
                '(SELECT STRING(fullVisitorId) + \'-\' + STRING(visitId) as user_session_id, trafficSource.referralPath AS referral, trafficSource.source AS source, trafficSource.keyword AS keyword, hits.customDimensions.value AS val, trafficSource.medium AS medium, hits.page.pagePath AS landing_page, hits.hitNumber AS hit_number '
                'FROM '
                'FLATTEN( '
                '(SELECT fullVisitorId, visitId, hits.hitNumber, trafficSource.referralPath, trafficSource.medium, trafficSource.source, trafficSource.keyword, hits.customDimensions.value, hits.page.pagePath  '
                'FROM TABLE_DATE_RANGE([95479039.ga_sessions_], TIMESTAMP(\'2015-01-01\'), TIMESTAMP(\'2015-10-01\'))) ,hits.hitNumber) '
                'WHERE '
                'hits.hitNumber = 1 '
                'AND  '
                'trafficSource.medium = "organic" '
                'AND '
                '( '
                'REGEXP_MATCH(hits.page.pagePath,"221616.com/search/") '
                ') '
                'AND '
                'REGEXP_MATCH(hits.page.pagePath,"221616.com/search/list") <> true '
                'AND '
                'REGEXP_MATCH(hits.page.pagePath,"221616.com/search/inquiry/over.html") <> true '
                'AND '
                'REGEXP_MATCH(hits.page.pagePath,"221616.com/search/proposals/confirm.html") <> true '
                'AND '
                'REGEXP_MATCH(hits.page.pagePath,"221616.com/search/favoriteList/index.html") <> true '
                'AND '
                'REGEXP_MATCH(hits.page.pagePath,"221616.com/search/index.html") <> true '
                'AND '
                'REGEXP_MATCH(hits.page.pagePath,"221616.com/search/select/index.html") <> true '
                'AND '
                'REGEXP_MATCH(hits.page.pagePath,"221616.com/search/proposals/overform.html") <> true '
                'AND '
                'REGEXP_MATCH(hits.page.pagePath,"221616.com/search/stock_inquiry_mail/index.html") <> true '
                ')  '
                'AS b '
                'ON a.user_session_id = b.user_session_id  '
                ') as c '
                '; '
                )
        }

        query_response = query_request.query(
            projectId=project_id,
            body=query_data).execute()
        # [END run_query]

        # [START print_results]
        print('Query Results:')
        # print query_response['rows'][0]
        # for row in query_response['rows']:
        #     print('\t'.join(field['v'] for field in row['f']))
        # [END print_results]

        with open("test.json","w") as f:
            json.dump(query_response['rows'], f, sort_keys=True, indent=4)

        # infile = open("test.json", "r")
        # outfile = open("test.csv", "w")
        # writer = csv.writer(outfile)
        # for row in json.loads(infile.read()):
        #     writer.write(row)


    except HttpError as err:
        print('Error: {}'.format(err.content))
        raise err


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('project_id', help='Your Google Cloud Project ID.')

    args = parser.parse_args()

    main(args.project_id)
