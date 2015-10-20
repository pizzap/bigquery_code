#!/usr/bin/env python
# -*- coding: utf-8 -*-

from oauth2client.client import SignedJwtAssertionCredentials
import gdata.spreadsheets.client

# 認証に必要な情報
client_email = "865248157142-e5h3hjm9h2kjbiggt7rmv6fc95ltithe@developer.gserviceaccount.com" # 手順2で発行されたメールアドレス
with open("bigquery-python-ca3e147753fe.p12") as f: private_key = f.read() # 手順2で発行された秘密鍵

# 認証情報の作成
scope = ["https://spreadsheets.google.com/feeds"]
credentials = SignedJwtAssertionCredentials(client_email, private_key,
    scope=scope)

# スプレッドシート用クライアントの準備
client = gdata.spreadsheets.client.SpreadsheetsClient()

# OAuth2.0での認証設定
auth_token = gdata.gauth.OAuth2TokenFromCredentials(credentials)
auth_token.authorize(client)

# ---- これでライブラリを利用してスプレッドシートにアクセスできる ---- #

# ワークシートの取得
sheets = client.get_worksheets("1F5opPDJ5CaAZyGwZrlfUlc4dso3sQ6KBdKIIK6970SA") # スプレッドシートIDを指定
for sheet in sheets.entry:
    print sheet.get_worksheet_id()

cell = client.get_cell("1F5opPDJ5CaAZyGwZrlfUlc4dso3sQ6KBdKIIK6970SA", "ontqp3p", 7, 2)
print cell.__dict__
print cell.title