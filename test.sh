#!/bin/sh
source ~/.pyenv2.7/bin/activate
gcloud auth login
python keyword_extraction.py cryptic-gate-92909
