#!/usr/bin/env python
# - - - - - - - - - - - - - - - - - - - - - # 
# Filename      : setup.py
# Purpose       : To setup pyTMS
# Date created  : Sat 19 Oct 2019 03:42:57 PM MDT
# Created by    : ck
# Last modified : Sat 19 Oct 2019 04:34:34 PM MDT
# Modified by   : ck
# - - - - - - - - - - - - - - - - - - - - - # 

from setuptools import setup

setup(
        name="pyTMS",
        version="1.0",
        description = "The Truven Market Scan Python Analysis Package",
        author = "Chong H. Kim",
        author_email = "chong@stratevi.com",
        pacakge = ["pyTMS"],
        install_requires = [
        "platform","subprocess","pandas","multiprocessing",
        "re","os","datetime","numpy"
        ],
        license = "MIT",
        platforms = "Linux",
        keywords = ["Truven Market Scan","Database analysis"]
        )
