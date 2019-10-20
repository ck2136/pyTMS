#!/usr/bin/env python
# - - - - - - - - - - - - - - - - - - - - - # 
# Filename      : test.py
# Purpose       :    
# Date created  : Wed 16 Oct 2019 10:11:17 AM MDT
# Created by    : ck
# Last modified : Sun 20 Oct 2019 04:50:23 PM MDT
# Modified by   : ck
# - - - - - - - - - - - - - - - - - - - - - # 

# Load Packages {{{
import numpy as np
import pandas as pd
from pyTMS import Study, CCI
from importlib import reload
reload(Study)
reload(CCI)
# }}}

## Study 1 {{{

### STEP 1: Instantiate study1 {{{
study1 = Study.Study()

### Set filepath
study1.filepath = "/home/ck/Downloads/"

### Identify inpatient and outpatient files
study1.ip_op_filelist() 

### Input DX and CPT codes
study1.ICD9 = ["347","34700","3470","34701","3471","34711","34710"]
study1.ICD10 = [
         "G474", "G4740","G47400","G4741","G47410",
         "G47411","G47419","G4742","G47420","G47421",
         "G47429"
         ]
study1.CPT = ["95782","95805","95808","95810"]

### }}}

### STEP 2: Extract patients from ip and op claims based on {{{
### The information provided above
study1.extract_ip_op_claims(group="case")

# At least 1 DX of the ICD9 and ICD10 
study1.case_claims
study1.case_ID

### Identify those with at least 2 DX
res = study1.identify_atleast2dx()

study1.case_claims.shape

res.shape
res.head()

### Store the current ones in self.case_ID
# study1.case_ID = res[["ENROLID"]]

### Identify those with 1 DX and 1 CPT where CPT before DX
res = study1.identify_al1cpt()

res.shape
### Store the current ones in self.case_ID
# study1.case_ID = study1.case_ID.append(res[["ENROLID"]])


### }}}

### STEP 3: Extract Control Group Without Conditions {{{

### CHECK CONDITIONS THEY SHOULD BE EMPTY
study1.ICD9_CTRL
study1.ICD10_CTRL

study1.identify_controls()

### Check if duplicate values exist
res = study1.control_ID
temp = res.groupby(["ENROLID"]).size().reset_index().rename(columns={0:"counts"})
temp[temp["counts"] > 1].shape

study1.control_claims
### NONE

### }}}

### STEP 4: Extract IP/OP Parameter Values for Case Group {{{

### Check case_ID control_ID to see if they are both populated
study1.case_ID
study1.control_ID
study1.regexpattern # will change when running next method

study1.extract_ip_op_parameters(group="case")

### Check the claims
study1.ep_ip_claims.shape
study1.ep_ip_claims.head

study1.ep_op_claims.shape
study1.ep_op_claims.head

### }}}

## STEP 5: Create Index SVCDATE Dataframe {{{
INDEX_DF = study1.create_svc_date_df(
            group = "case"
            )

INDEX_DF.shape
INDEX_DF.head()

## }}}

## STEP 6: Extract All Enrolmment Data for group {{{
ENROL_DF = study1.extract_enroll_claims(
            study1.process_enroll_chunk_all,
            store = False,
            cols = study1.enrl_cols,
            idonly = False,
            group = "case"
            )
ENROL_DF

## }}}

## STEP 7: Apply Continuous Enrollment Criteria For Each Group{{{

### MERGE INDEX_DF and ENROL_DF
NEWENROL_DF = study1.Merge_IDX_ENR(
            INDEX_DF, 
            ENROL_DF
            )

NEWENROL_DF

### CREATE ENROLLMNT DF
ENROL_FIN = pd.DataFrame()
study1.Recursive_Extract_CI_IDs(
            NEWENROL_DF,
            ENROL_FIN, # a empty pd.DataFrame() needs to be an input as recursively it goes in and stuff gets added
            repeat = 0, # how many times this function has been called
            strict = True
            )

### Check the continuous enrollment data frame
study1.CE_df

## }}}

## STEP 8: Extract Demographic + CCI variables {{{

### Extract Demographic Variable
study1.extract_demo(
            group = "case"
            )

study1.demo_df

study1.case_ID

### Extract CCI Variable


## }}}

## MISCELLANEOUS CODE{{{

for chunk in  pd.read_csv(study1.ip_op_filelist()[0], 
        compression="gzip",
        chunksize=1000000,
        dtype = {
                "ENROLID": np.int64, "DX1": np.character,"DX2": np.character, "DX3": np.character, "DX4": np.character, 
                "PROC1": np.character, "HLTHPLAN": np.character, "NTWKPROV": np.character, "PROCTYP": np.character, 
                "PAIDNTWK": np.character, "PDX": np.character, "INDUSTRY": np.character, "CASE": np.int8, "PROVID": np.float64,
                "QTY": np.int64, "COB": np.float64, "CAP_SVC": np.character, "PROCMOD":np.character, "PROCGRP":np.float64,
                "PPROC":np.character
                }
        ):

    print(chunk.shape)


np.float16

temp = pd.read_csv(study1.ip_op_filelist()[0], 
        compression="gzip",
        nrows = 10000,
        dtype = {
                "ENROLID": np.int64, "DX1": np.character,"DX2": np.character, "DX3": np.character, "DX4": np.character, 
                "PROC1": np.character, "HLTHPLAN": np.character, "NTWKPROV": np.character, "PROCTYP": np.character, 
                "PAIDNTWK": np.character, "PDX": np.character, "INDUSTRY": np.character, "CASE": np.float16, "PROVID": np.float64,
                "QTY": np.int64, "COB": np.float64, "CAP_SVC": np.character, "PROCMOD":np.character, "PROCGRP":np.float64,
                "PPROC":np.character, "WGTKEY":np.float64
                }
        )

temp.shape
temp.columns[83]

len(temp) == 0

import re
study1.ip_op_filelist()

cciobj = CCI.CCI()

if re.search('(ccae.*){1}(\d){3}', study1.ip_op_filelist(printfiles=False)[1]):
    print("HELLO")



df1 = pd.DataFrame({
    "ENROLID":[1,2],
    "SVCDATE":["10/20/2013","10/25/2013"],
    }).set_index("ENROLID").rename(columns={"SVCDATE":"SVCDATE_10"})

df2 = pd.DataFrame({
    "ENROLID":[1,2],
    "SVCDATE":["11/20/2013","12/25/2013"],
    }).set_index("ENROLID").rename(columns={"SVCDATE":"SVCDATE_9"})

df1["SVCDATE_10"] = pd.to_datetime(df1["SVCDATE_10"])
df2["SVCDATE_9"] = pd.to_datetime(df2["SVCDATE_9"])
df2 = pd.DataFrame({"ENROLID":[1,2]})
## }}}

## }}}
