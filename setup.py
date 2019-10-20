#!/usr/bin/env python
# - - - - - - - - - - - - - - - - - - - - - # 
# Filename      : setup.py
# Purpose       : To setup pyTMS
# Date created  : Sat 19 Oct 2019 03:42:57 PM MDT
# Created by    : ck
# Last modified : Sat 19 Oct 2019 05:51:25 PM MDT
# Modified by   : ck
# - - - - - - - - - - - - - - - - - - - - - # 

from setuptools import setup, find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
        name="pyTMS",
        version="1.0",
        description = "The Truven Market Scan Python Analysis Package",
        author = "Chong H. Kim",
        author_email = "chong@stratevi.com",
        package = ["pyTMS"],
        install_requires = [
        "platform","subprocess","pandas","multiprocessing",
        "re","os","datetime","numpy"
        ],
        license = "MIT",
        platforms = "Linux",
        packages=find_packages(),
        keywords = ["Truven Market Scan","Database analysis"],
        classifiers = [
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Analysts",
            "Topic :: Data Analysis :: Data Management Tools",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7"
            ],
        )
