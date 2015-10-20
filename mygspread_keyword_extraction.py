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

sht = gc.open_by_key("1sZFjbGpyhtIFKJVo0OyfFCSrjIa0a8Oc_avbTeAtkqQ")
wsht = sht.get_worksheet(0) 

for x in range(2, 154):
	# val = "http://" + wsht.cell(x, 11).value
	# s = "index.html"
	# val = val.replace(s,"")

	# page = requests.get(val)
	# soup = BeautifulSoup(page.text.encode(page.encoding),"html.parser")

	# desc = soup.find(attrs={"name":"keywords"})

	# pattern = 'content=\"..*\"'
	# res = re.search(pattern, str(desc))

	res = wsht.cell(x, 1).value
	ex = re.compile(r".*中古車.*|.*ガリバー.*|.*Gulliver.*|.*toyota.*|.*honda.*|.*nissan.*|.*mazda.*|.*subaru.*|.*suzuki.*|.*mitsubishi.*|.*daihatsu.*|.*volkswagen.*|.*トヨタ.*|.*日産.*|.*ニッサン.*|.*本田.*|.*ホンダ.*|.*スバル.*|.*マツダ.*|.*スズキ.*|.*フォルクスワーゲン.*|.*ダイハツ.*|.*三菱.*|.*ミツビシ.*|.*BMW.*|.*在庫.*|.*車.*|.*カタログ.*|.*モデル.*|.*装備.*|.*スペック.*|.*検索.*|.*ボディタイプ.*|.*燃費.*|.*ID.*", re.I)

	if res:
		words = res
		l = ""
		# print x, 	
		for w in words.split():
			if ex.search(w):
				print w
				pass
			else:
				l = l + w + " "
		print ""
		wsht.update_cell(x, 2, l)
	else:
		print "fail"
