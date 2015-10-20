#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import gspread
from oauth2client.client import SignedJwtAssertionCredentials
from bs4 import BeautifulSoup
import urllib
import requests
import re

json_key = json.load(open('bigquery-python2-f7a2730baff3.json'))
scope = ['https://spreadsheets.google.com/feeds']

credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'], scope)

gc = gspread.authorize(credentials)

sht = gc.open_by_key("19qB2oUHhgo2QJqyWb8Rht8OF1DTAdRFzZUwifulTnVI")
wsht = sht.get_worksheet(0) 

for x in range(2, 154):
	val = "http://" + wsht.cell(x, 11).value
	s = "index.html"
	val = val.replace(s,"")

	# page = urllib.urlopen(val).read()
	page = requests.get(val)
	soup = BeautifulSoup(page.text.encode(page.encoding),"html.parser")

	# print x, soup.html.head.title

	desc = soup.find(attrs={"name":"keywords"})
	pattern = 'content=\"..*\"'
	res = re.search(pattern, str(desc))
	ex = re.compile(r".*中古車.*|.*ガリバー.*|.*Gulliver.*|.*toyota.*|.*honda.*|.*nissan.*|.*mazda.*|.*subaru.*|.*suzuki.*|.*mitsubishi.*|.*daihatsu.*|.*volkswagen.*|.*トヨタ.*|.*日産.*|.*ニッサン.*|.*本田.*|.*ホンダ.*|.*スバル.*|.*マツダ.*|.*スズキ.*|.*フォルクスワーゲン.*|.*ダイハツ.*|.*三菱.*|.*ミツビシ.*|.*BMW.*|.*在庫.*|.*車.*|.*カタログ.*|.*モデル.*|.*装備.*|.*スペック.*|.*検索.*|.*ボディタイプ.*|.*燃費.*|.*ID.*", re.I)

	if res:
		words = res.group().split('\"')[1].split("|")[0].split(",1ページ")[0]
		l = ""
		print x, 	
		for w in words.split(","):
			if ex.search(w):
				pass
			else:
				print w,
				l = l + w + " "
		print ""
		wsht.update_cell(x, 10, l.decode('utf-8'))
	else:
		print "fail"
