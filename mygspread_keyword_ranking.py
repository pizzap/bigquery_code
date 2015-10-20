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

sht = gc.open_by_key("1O7homO1bBkL2p_JlSB1j0lJKY1VVx1a-nXceYDchFAM")
wsht = sht.get_worksheet(0) 

