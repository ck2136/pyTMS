#!/usr/bin/env python
# - - - - - - - - - - - - - - - - - - - - - # 
# Filename      : test.py
# Purpose       :    
# Date created  : Wed 16 Oct 2019 10:11:17 AM MDT
# Created by    : ck
# Last modified : Sat 19 Oct 2019 03:40:02 PM MDT
# Modified by   : ck
# - - - - - - - - - - - - - - - - - - - - - # 

# Load Packages {{{
import numpy as np
import pandas as pd
from pyTMS import Study
from importlib import reload
reload(Study)
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
            study1.process_ctrlgrp_enroll_chunk_all,
            store = False,
            cols = study1.enrl_cols,
            idonly = False,
            group = "case"
            )
ENROL_DF

## }}}

## STEP 7: Apply Continuous Enrollment Criteria For Each Group{{{

# UTILITY FUNCTION 1
def label_DTSTART(row):
    if pd.isnull(row["DTSTART_y"]) == True:
        return row["DTSTART_x"]
    else:
        return row["DTSTART_y"]

### UTILITY FUNCTION 2- Condition 1: Individuals must have at least the same number of rows if not then they just don't have enough enrollment data so not continuous enrolled 
def CE_condition1(x, months):
    if len(x.columns) < months:
        return 0
    else:
        return 1

# MAIN FUNCTION
def extract_id_ci(INDEX_DF, ENROL_DF, months=12):
    ## Merge index dates with enrollment df
    temp = INDEX_DF.reset_index()[["ENROLID","SVCDATE_INDEX"]].merge(ENROL_DF)

    ## converge DTSTART and DTEND variables to datetime object
    temp["DTSTART"] = pd.to_datetime(temp["DTSTART"]) 
    temp["DTEND"] = pd.to_datetime(temp["DTEND"]) 

    ### Identify DTEND's that are geq than Index Date for each ENROLID
    INDEX_DF1 = temp.groupby("ENROLID").apply(lambda row: row[row.SVCDATE_INDEX<=row.DTEND]).set_index("ENROLID").sort_values("DTEND").groupby("ENROLID").first().reset_index()

    ### Change the DTSTART to SVCDATE_INDEX
    INDEX_DF1['DTSTART'] = INDEX_DF1['SVCDATE_INDEX']

    ### Merge back the one row for each of the ENROLID
    NEWENROL_DF = temp.merge(INDEX_DF1[["ENROLID","DTSTART","DTEND"]], on=["ENROLID","DTEND"], how="left")

    ### CHANGE DTSTART to The SVCDATE_INDEX 
    NEWENROL_DF["DTSTART"] = NEWENROL_DF.apply(lambda row: label_DTSTART(row), axis=1)

    ### RECALCULATE MEMDAYS
    NEWENROL_DF["MEMDAYS_new"] = NEWENROL_DF["DTEND"] - NEWENROL_DF["DTSTART"]
    NEWENROL_DF["MEMDAYS_new"] = NEWENROL_DF.MEMDAYS_new.dt.days

    ### Sort by DTEND ASCENDING (from beginning to end) and select 12 rows
    ENROL = NEWENROL_DF.groupby("ENROLID").apply(lambda id: id.sort_values(["DTEND"])).reset_index(drop=True).groupby("ENROLID").head(months)

    ENROL_WIDE = pd.DataFrame()

    # IMPOSE CONDITION 1
    ENROL_WIDE["CE1"] = ENROL.groupby("ENROLID").apply(lambda x: CE_condition1(x,12))

    # IMPOSE CONDITION 2
    ENROL_WIDE["CE2"] = np.abs(((ENROL.groupby("ENROLID").last().DTEND - ENROL.groupby("ENROLID").first().DTSTART) - pd.Timedelta(pd.Timedelta(str(365.24/12 * months) + " days"))).dt.days) <= 45

    return(
            ENROL_WIDE[((ENROL_WIDE["CE1"] == 1) & (ENROL_WIDE["CE2"] == True))]
            )



extract_id_ci(INDEX_DF, ENROL_DF, months=12)

### MERGE THE INDEX CLAIMS with ENROLLMENT
temp = INDEX_DF.reset_index()[["ENROLID","SVCDATE_INDEX"]].merge(ENROL_DF)

### Identify first row that is lower than the SVCDATE_INDEX

#### Change DTSTART AND DTEND to datetime objects
temp["DTSTART"] = pd.to_datetime(temp["DTSTART"]) 
temp["DTEND"] = pd.to_datetime(temp["DTEND"]) 

### 1. Identify DTEND's that are geq than Index Date for each ENROLID
INDEX_DF1 = temp.groupby("ENROLID").apply(lambda row: row[row.SVCDATE_INDEX<=row.DTEND]).set_index("ENROLID").sort_values("DTEND").groupby("ENROLID").first().reset_index()

### Change the DTSTART to SVCDATE_INDEX
INDEX_DF1['DTSTART'] = INDEX_DF1['SVCDATE_INDEX']

### Merge back the one row for each of the ENROLID
NEWENROL_DF = temp.merge(INDEX_DF1[["ENROLID","DTSTART","DTEND"]], on=["ENROLID","DTEND"], how="left")

NEWENROL_DF[["ENROLID","DTSTART_x","DTSTART_y"]]

### CHANGE DTSTART to The SVCDATE_INDEX 

def label_DTSTART(row):
    if pd.isnull(row["DTSTART_y"]) == True:
        return row["DTSTART_x"]
    else:
        return row["DTSTART_y"]
    
NEWENROL_DF["DTSTART"] = NEWENROL_DF.apply(lambda row: label_DTSTART(row), axis=1)

### RECALCULATE MEMDAYS
NEWENROL_DF["MEMDAYS_new"] = NEWENROL_DF["DTEND"] - NEWENROL_DF["DTSTART"]
NEWENROL_DF["MEMDAYS_new"] = NEWENROL_DF.MEMDAYS_new.dt.days

### Sort by DTEND ASCENDING (from beginning to end) and select 12 rows
ENROL12 = NEWENROL_DF.groupby("ENROLID").apply(lambda id: id.sort_values(["DTEND"])).reset_index(drop=True).groupby("ENROLID").head(12)
ENROL24 = NEWENROL_DF.groupby("ENROLID").apply(lambda id: id.sort_values(["DTEND"])).reset_index(drop=True).groupby("ENROLID").head(24)
ENROL36 = NEWENROL_DF.groupby("ENROLID").apply(lambda id: id.sort_values(["DTEND"])).reset_index(drop=True).groupby("ENROLID").head(36)
ENROL48 = NEWENROL_DF.groupby("ENROLID").apply(lambda id: id.sort_values(["DTEND"])).reset_index(drop=True).groupby("ENROLID").head(48)
ENROL60 = NEWENROL_DF.groupby("ENROLID").apply(lambda id: id.sort_values(["DTEND"])).reset_index(drop=True).groupby("ENROLID").head(60)

### Check if continuous enrolled for 12 months 

### Condition 1: Individuals must have at least the same number of rows if not then they just don't have enough enrollment data so not continuous enrolled 
def CE_condition1(x, months):
    if len(x.columns) < months:
        return 0
    else:
        return 1

ENROL_WIDE = pd.DataFrame()
ENROL_WIDE["CE_MIN12"] = ENROL12.groupby("ENROLID").apply(lambda x: oneyr_ce1(x,12))
ENROL_WIDE["CE_MIN24"] = ENROL24.groupby("ENROLID").apply(lambda x: oneyr_ce1(x,24))
ENROL_WIDE["CE_MIN36"] = ENROL36.groupby("ENROLID").apply(lambda x: oneyr_ce1(x,36))
ENROL_WIDE["CE_MIN48"] = ENROL48.groupby("ENROLID").apply(lambda x: oneyr_ce1(x,48))
ENROL_WIDE["CE_MIN60"] = ENROL60.groupby("ENROLID").apply(lambda x: oneyr_ce1(x,60))

ENROL_WIDE.head(5)

## Condition 2: Identify id's where difference in 12 months last DT to first claim is greater than 365 days... If the difference is greater than 365 that means there is a gap
## in coverage
ENROL_WIDE["GAP_12"] = np.abs(((ENROL12.groupby("ENROLID").last().DTEND - ENROL12.groupby("ENROLID").first().DTSTART) - pd.Timedelta("365 days")).dt.days) <= 45
ENROL_WIDE["GAP_24"] = np.abs(((ENROL24.groupby("ENROLID").last().DTEND - ENROL24.groupby("ENROLID").first().DTSTART) - pd.Timedelta("730 days")).dt.days) <= 45
ENROL_WIDE["GAP_36"] = np.abs(((ENROL36.groupby("ENROLID").last().DTEND - ENROL36.groupby("ENROLID").first().DTSTART) - pd.Timedelta("1095 days")).dt.days) <= 45
ENROL_WIDE["GAP_48"] = np.abs(((ENROL48.groupby("ENROLID").last().DTEND - ENROL48.groupby("ENROLID").first().DTSTART) - pd.Timedelta("1460 days")).dt.days) <= 45
ENROL_WIDE["GAP_60"] = np.abs(((ENROL60.groupby("ENROLID").last().DTEND - ENROL60.groupby("ENROLID").first().DTSTART) - pd.Timedelta("1825 days")).dt.days) <= 45

CE_1YRID = ENROL_WIDE[((ENROL_WIDE["CE_MIN12"] == 1) & (ENROL_WIDE["GAP_12"] == True))]
CE_2YRID = ENROL_WIDE[((ENROL_WIDE["CE_MIN24"] == 1) & (ENROL_WIDE["GAP_24"] == True))]
CE_3YRID = ENROL_WIDE[((ENROL_WIDE["CE_MIN36"] == 1) & (ENROL_WIDE["GAP_36"] == True))]
CE_4YRID = ENROL_WIDE[((ENROL_WIDE["CE_MIN48"] == 1) & (ENROL_WIDE["GAP_48"] == True))]
CE_5YRID = ENROL_WIDE[((ENROL_WIDE["CE_MIN60"] == 1) & (ENROL_WIDE["GAP_60"] == True))]

CE_1YRID

### Condition 2: The gap between DTSTART_MIN and DTSTART_MAX should not exceed 45 days
def oneyr_ce2(x):

    if x.last().DTEND  - x.first().DTSTART 
    if len(x.columns) < 12:
        return 0
    else:
        return 1

### UTILITY FUNCTION 3- Condition 3: SUM MEMBER DAYS
def CE_condition3(self,x,months):
    if sum(x["MEMDAYS_new"]) + 45 <= 365.24/12 * months:
        return True
    else:
        return False

temp.head(13)

NEWENROL_DF.groupby("ENROLID").apply


NEWENROL_DF.DTSTART_y[0] == np.datetime64("NaT")

NEWENROL_DF.loc[NEWENROL_DF["DTSTART_y"].empty != True , "DTSTART"] = NEWENROL_DF["DTSTART_y"]

NEWENROL_DF.groupby("ENROLID").apply(lambda x: sum(x["MEMDAYS_new"]))


INDEX_DF1.head()

### 1. Identify DTEND's that are leq than Index Date for each ENROLID
INDEX_DF1 = temp.groupby("ENROLID").apply(lambda row: row[row.SVCDATE_INDEX>=row.DTEND]).set_index("ENROLID").sort_values("DTEND").groupby("ENROLID").last()
INDEX_DF1.shape

temp.groupby("ENROLID").apply(lambda row: )

### 2. Change the first row of DTEND to that of the SVCDATE_INDEX

### 2. Select the first row of ID that indicates index date is lower than DTEND

INDEX_DF1.head()

### 

temp[["ENROLID","SVCDATE_INDEX","DTEND"]]

temp[temp["SVCDATE_INDEX"]>=temp["DTEND"]]

## }}}

## SSet Evaluable Population Using Continuous Enrollment {{{

## }}}

## MISCELLANEOUS CODE{{{
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
