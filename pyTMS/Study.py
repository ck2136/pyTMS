#!/usr/bin/env python
# - - - - - - - - - - - - - - - - - - - - - # 
# Filename      : Study.py
# Purpose       : class Study
# Date created  : Wed 16 Oct 2019 09:50:10 AM MDT
# Created by    : ck
# Last modified : Tue 22 Oct 2019 11:05:25 AM MDT
# Modified by   : ck
# - - - - - - - - - - - - - - - - - - - - - # 

# Baseline Packagse  {{{
import platform
import subprocess
import pandas as pd # for data munging
import multiprocessing as mp # for parallelizing the chunk reading and processing
import re # for pattern matching *.csv.gz files in the folder
import os # for listing all files in folder and pattern matching
import datetime 
import sys
import numpy as np

# }}}

class Study(object):

    # DOCSTRING {{{

    """STUDY DESCRIPTION"""

    # }}}

    # CONSTRUCTOR {{{

    def __init__(self):
        """
        Attributes: 

            studyname: Name of Study"
            case_ID: ID's of case 
            control_ID: ID's of control
            case_claims: DX and SVCDATE for cases
            control_claims: DX and SVCDATE for cases
            ICD9: List of ICD9's for Extracting disease_pop
            ICD10: List of ICD10's for Extracting disease_pop
            CPT: List of CPT codes for Extracting disease_pop
            filepath: filepath to extract the data
                Usually the filename will start with 
                ccae* for commercial
                mdcr* for medicare supplemental
                We will assume that these are the files that we are getting
                Also we will assume that the extension will be *.csv.gz

        """
        self.name = "Default Study Name"
        self.case_ID = pd.DataFrame()
        self.control_ID = pd.DataFrame()
        self.case_claims = pd.DataFrame()
        self.control_claims = pd.DataFrame()
        self.ep_ip_claims = pd.DataFrame()
        self.ep_op_claims = pd.DataFrame()
        self.CE_df = pd.DataFrame()
        self.demo_df = pd.DataFrame()

        # DX and CPT codes to extract
        self.ICD9 = ["347","34700","3470","34701","3471","34711","34710"] # for narcolepsy
        self.ICD10 = [
         "G474", "G4740","G47400","G4741","G47410",
         "G47411","G47419","G4742","G47420","G47421",
         "G47429"
         ]
        self.CPT = ["95782","95805","95808","95810"]
        self.ICD9_CTRL = []
        self.ICD10_CTRL = []
        self.CPT_CTRL = []
        self.dtype = {
                "ENROLID": np.int64, "DX1": np.character,"DX2": np.character, "DX3": np.character, "DX4": np.character, 
                "PROC1": np.character, "HLTHPLAN": np.character, "NTWKPROV": np.character, "PROCTYP": np.character, 
                "PAIDNTWK": np.character, "PDX": np.character, "INDUSTRY": np.character, "CASE": np.float16, "PROVID": np.float64,
                "QTY": np.int64, "COB": np.float64, "CAP_SVC": np.character, "PROCMOD":np.character, "PROCGRP":np.float64,
                "PPROC":np.character
                }

        # For processing the claims data
        self.filepath = ""
        self.filelist = []
        self.regexpattern = "(ccae|mdcr){1}(s|o){1}(\d){3}.csv.gz"
        self.cores = mp.cpu_count()
        self.chunksize = 250000
        self.filter_columns_ip = ["ENROLID", "SVCDATE","PDX","DX1","DX2","DX3","DX4","PROC1"]
        self.filter_columns_op = ["ENROLID", "SVCDATE","DX1","DX2","DX3","DX4","PROC1"]
        self.filter_columns_ip_full = ["ENROLID", "SVCDATE","PDX","DX1","DX2","DX3","DX4","PROC1","YEAR","CASEID","PROCTYP","PROCMOD","PAY","STDPLAC","STDPROV","SVCSCAT"]
        self.filter_columns_op_full = ["ENROLID", "SVCDATE","DX1","DX2","DX3","DX4","PROC1","YEAR","PROCTYP","PROCMOD","PAY","STDPLAC","STDPROV","SVCSCAT"]
        self.filter_columns_demo = ["ENROLID","YEAR","AGE","DOBYR","AGEGRP","SEX","REGION","EFAMID","EMPREL"]
        self.enrl_cols = ["ENROLID","DTSTART","DTEND","MEMDAYS"]
        self.sex_labels = {
                    "1": "1-Male",
                    "2": "2-Female"
                }
        self.region_labels = {
                    "1": "1-Northeast",
                    "2": "2-North Central",
                    "3": "3-South",
                    "4": "4-West",
                    "5": "5-Unknown"
                }

    # }}}

    # METHODS {{{

    ## STEP 1 : LIST OF IP/OP *csv.gz files to extract {{{
    def ip_op_filelist(
            self, 
            printfiles=True,
            ):

        listoffiles = [self.filepath + f for f in os.listdir(self.filepath) if re.search(self.regexpattern,f)]

        if printfiles:
            print("Inpatient and Outpatient Claims identified are : ", listoffiles)

        return(listoffiles)

    ## }}}

    ## STEP 2 : Parallel Extraction of IP/OP Claims files {{{

    def extract_ip_op_claims(
            self,
            group = "case"
            ):

        filelist = self.ip_op_filelist(
                printfiles=False
                )

        df_fin = pd.DataFrame()

        if group == "case":

            DX_LIST = [(9,dx) for dx in self.ICD9] + [(0,dx) for dx in self.ICD10]
            arglist = list(
                    zip(
                        filelist,
                        [self.chunksize  for x in range(len(filelist))],
                        [self.CPT for x in range(len(filelist))],
                        [self.ICD9  for x in range(len(filelist))],
                        [DX_LIST  for x in range(len(filelist))],
                        [self.filter_columns_ip for x in range(len(filelist))],
                        [self.filter_columns_op for x in range(len(filelist))]
                        )
                    )
        elif group == "control":

            DX_LIST = [(9,dx) for dx in self.ICD9_CTRL] + [(0,dx) for dx in self.ICD10_CTRL]
            arglist = list(
                    zip(
                        filelist,
                        [self.chunksize  for x in range(len(filelist))],
                        [self.CPT_CTRL for x in range(len(filelist))],
                        [self.ICD9_CTRL  for x in range(len(filelist))],
                        [DX_LIST  for x in range(len(filelist))],
                        [self.filter_columns_ip for x in range(len(filelist))],
                        [self.filter_columns_ip for x in range(len(filelist))]
                        )
                    )
        else:
            return(
                    print("Options for extracting claims: 'case' or 'control'")
                    )


        # Init parallel processes
        pool = mp.Pool(self.cores)
        results_obj = [pool.apply_async(self.process_ip_op_chunk, 
            args = (
                i, 
                argtup[0],
                argtup[1],
                argtup[2],
                argtup[3],
                argtup[4],
                argtup[5],
                argtup[6]
                )
            ) for i,argtup in enumerate(arglist)
            ]
        # Don't forget to close the processors
        pool.close()
        results_fin = [r.get()[1] for r in results_obj]
        df_fin = df_fin.append(pd.concat(results_fin))

        # return results 

        print("FINISHED COMPILING ", group, " claims!")
        print("Results are claims data + ID of ", group, "!")
        self.case_ID = df_fin.groupby("ENROLID", as_index=False).size().reset_index()[["ENROLID"]]
        self.case_claims = df_fin
        # self.case_claims = return((df_fin,df_fin.groupby("ENROLID", as_index=False).size().reset_index()[["ENROLID"]]))

    ## }}}

    ## STEP 2 : Filter/Processing Individual IP/OP files {{{

    def process_ip_op_chunk(
            self,
            i, 
            data, 
            chunksize,
            CPT_LIST,
            DX_LIST_PRE,
            DX_LIST,
            filter_columns_ip,
            filter_columns_op
            ):
        """
    process_ip_op_chunk: 

        Function/method to process individual chunks to extract inpatient services
        and outpatient claims to get at the exact number of ID's 

    Sections in The Code

    Section 1: General Setup
    Section 2: Read Chunk of Data
    Section 2: Include Patients using DX and CPT

    Arguments: 

        data = file path/name
        chunksize = Chunk size to extract data by
        CPT_LIST = List of CPT codes (e.g. ["1234","124512"]
        DX_LIST_PRE = Diagnosis list pre 2015 (e.g. ['3523','32342','34342']
        DX_LIST = Diagnosis list post 2015. This has to be a list of tuples where the first item in the tuple
                    is a 0 for ICD10 and 9 for ICD9. (e.g. [(0, '3523'),(9, '32342')]
    """

        # - - - - - - - - - - - - - - - - - -
        # Section 1: General Setup
        # - - - - - - - - - - - - - - - - - -

        # Create placeholder dataframe
        df_fin = pd.DataFrame()

        # - - - - - - - - - - - - - - - - - -
        # Section 2: Read Chunk of Data
        # - - - - - - - - - - - - - - - - - -

        c = 1
        # Loop through chunks 
        for chunk in pd.read_csv(data, 
                compression="gzip",
                chunksize=chunksize,
                dtype = self.dtype
                ):

            # - - - - - - - - - - - - - - - - - -
        # Section 3: Include Patients using DX and CPT
        # - - - - - - - - - - - - - - - - - -

            # - - - - - - - - - - - - - - - - - -
            # 3.1: PRE 2015
            # - - - - - - - - - - - - - - - - - -

            # If filename contains 133|144 that means its pre 2015 therefore only ICD9 DX exists thus no DXVER variable
            if re.search('(133|143)', data):
                if {"PDX"}.issubset(chunk.columns):
                    chunk = chunk[filter_columns_ip]
                else:
                    chunk = chunk[filter_columns_op]
                # DX LIST FOR < 2015
                DX_LIST = DX_LIST_PRE

                # For DX Check
                for j in range(len(DX_LIST)):
                    chunk["DX_FLAG"] = False
                    if {"PDX"}.issubset(chunk.columns):
                        chunk["DX_FLAG"] = ((chunk.PDX == DX_LIST[j]) | (chunk.DX1 == DX_LIST[j]) | (chunk.DX1 == DX_LIST[j]) | (chunk.DX2 == DX_LIST[j]) | (chunk.DX3 == DX_LIST[j]) | (chunk.DX4 == DX_LIST[j]))
                    else:
                        chunk["DX_FLAG"] = ((chunk.DX1 == DX_LIST[j]) | (chunk.DX1 == DX_LIST[j]) | (chunk.DX2 == DX_LIST[j]) | (chunk.DX3 == DX_LIST[j]) | (chunk.DX4 == DX_LIST[j]))
                    # Append rows to final data
                    df_fin = df_fin.append(chunk[(chunk["DX_FLAG"]==True)])

                # For Procedure Check
                for j in range(len(CPT_LIST)):
                    chunk["CPT_FLAG"] = False
                    chunk["CPT_FLAG"] = ((chunk.PROC1 == CPT_LIST[j]))
                    # Append rows to final data
                    df_fin = df_fin.append(chunk[(chunk["CPT_FLAG"]==True)])

        # - - - - - - - - - - - - - - - - - -
        # 3.2: POST 2015
        # - - - - - - - - - - - - - - - - - -

            # If filename does not contains 133|144 that means its post 2015 therefore ICD9+10 exists thus claims data
            # contains the DXVER variable
            else:
                if {"PDX"}.issubset(chunk.columns):
                    chunk = chunk[filter_columns_ip]
                else:
                    chunk = chunk[filter_columns_op]

                # For DX Check after 2015
                for j in range(len(DX_LIST)):
                    chunk["DX_FLAG"] = False
                    if DX_LIST[j][0] in list(chunk.groupby("DXVER").size().index.values):
                        if {"PDX"}.issubset(chunk.columns):
                            chunk["DX_FLAG"] = (chunk["DXVER"]==DX_LIST[j][0]) & ((chunk.PDX == DX_LIST[j][1]) | (chunk.DX1 == DX_LIST[j][1]) | (chunk.DX1 == DX_LIST[j][1]) | (chunk.DX2 == DX_LIST[j][1]) | (chunk.DX3 == DX_LIST[j][1]) | (chunk.DX4 == DX_LIST[j][1]))
                        else:
                            chunk["DX_FLAG"] = (chunk["DXVER"]==DX_LIST[j][0]) & ((chunk.DX1 == DX_LIST[j][1]) | (chunk.DX1 == DX_LIST[j][1]) | (chunk.DX2 == DX_LIST[j][1]) | (chunk.DX3 == DX_LIST[j][1]) | (chunk.DX4 == DX_LIST[j][1]))

                    # Append rows to final data
                    df_fin = df_fin.append(chunk[(chunk["DX_FLAG"]==True)])

                # For Procedure Check
                for j in range(len(CPT_LIST)):
                    chunk["CPT_FLAG"] = False
                    chunk["CPT_FLAG"] = ((chunk.PROC1 == CPT_LIST[j]))
                    # Append rows to final data
                    df_fin = df_fin.append(chunk[(chunk["CPT_FLAG"]==True)])

            print("Finished Chunk #", c ," and rows added: ", df_fin.shape[0]) 
            c += 1

        # - - - - - - - - - - - - - - - - - -
        # Section 4: Return Filtered Data
        # - - - - - - - - - - - - - - - - - -
        return(i, df_fin)
    
    ## }}}

    ## STEP 2 : Identify at least 2 DX {{{
    def identify_atleast2dx(
            self
            ):
        """
    identify_atleast2dx: 

        Function/method to identify individuals with at least 2 dx of 
        what is included in the ICD9 and ICD10 

    """

        # - - - - - - - - - - - - - - - - - -
        # Identify ID's with Greater than 1 dx of disease
        # - - - - - - - - - - - - - - - - - -
        id_2dx = self.case_claims.groupby(["ENROLID","DX_FLAG"]).size().reset_index(name="counts")
        id_2dx = id_2dx[(id_2dx["DX_FLAG"] == True) & (id_2dx["counts"] > 1)]

        return(id_2dx)

    ## }}}

    ## STEP 2 : Identify 1 DX and at least 1 CPT {{{
    def identify_al1cpt(
            self
            ):
        """
    identify_al1cpt: 

        Function/method to identify individuals with 1 dx of DISEASE
        and at least 1 CPT code for the disease

    """

        # - - - - - - - - - - - - - - - - - -
        # Identify ID's with 1 dx of disease
        # - - - - - - - - - - - - - - - - - -
        id_1dx = self.case_claims.groupby(["ENROLID","DX_FLAG"]).size().reset_index(name="counts")
        id_1dx = id_1dx[(id_1dx["DX_FLAG"] == True ) & (id_1dx["counts"] == 1)] 
        print("Number of subjects with 1 DX : ", id_1dx.shape[0])

        # - - - - - - - - - - - - - - - - - -
        # DX DATE FRAME
        # - - - - - - - - - - - - - - - - - -
        df_1dx_dxdate = id_1dx[["ENROLID"]].merge(self.case_claims[["ENROLID","DX_FLAG","SVCDATE"]], how = "inner")
        df_1dx_dxdate = df_1dx_dxdate[df_1dx_dxdate["DX_FLAG"] == True][["ENROLID","SVCDATE"]].rename(columns={"SVCDATE":"SVCDATE_DX"})

        # - - - - - - - - - - - - - - - - - -
        # CPT DATE FRAME
        # - - - - - - - - - - - - - - - - - -
        df_1dx_cptdate = id_1dx[["ENROLID"]].merge(self.case_claims[["ENROLID","CPT_FLAG","SVCDATE"]], how = "inner")
        df_1dx_cptdate = df_1dx_cptdate[df_1dx_cptdate["CPT_FLAG"] == True]
        df_1dx_cptdate = df_1dx_cptdate.sort_values(by = ["ENROLID","SVCDATE"]).groupby("ENROLID").first().reset_index()[["ENROLID","SVCDATE"]].rename(columns={"SVCDATE":"SVCDATE_CPT"})

        # - - - - - - - - - - - - - - - - - -
        # MERGE DX AND CPT DATE FRAMES
        # - - - - - - - - - - - - - - - - - -
        df_1dx_al1cpt = df_1dx_cptdate.merge(df_1dx_dxdate)
        df_1dx_al1cpt["delta"] = pd.to_datetime(df_1dx_al1cpt["SVCDATE_CPT"]) - pd.to_datetime(df_1dx_al1cpt["SVCDATE_DX"]) 

        # - - - - - - - - - - - - - - - - - -
        # Keep only subjects that have first CPT date before DX date
        # - - - - - - - - - - - - - - - - - -
        id_1dx_al1cpt = df_1dx_al1cpt[df_1dx_al1cpt["delta"] < datetime.timedelta(days = 0)][["ENROLID"]]

        print("Number of subjects with 1 DX of Disease and at least 1 CPT Procedure : ", id_1dx_al1cpt.shape[0])

        return(id_1dx_al1cpt)

    ## }}}

    ## STEP 3 : Identify Controls {{{
    def identify_controls(
            self,
            N = 100000 # only when we don't have ICD9_CTRL and ICD10_CTRL
            ):

        """
        identify_controls:
            Function/Method to extract Control Population.

        Arguments:
            
            N :     Number of patients to be extracted if no ICD9_CTRL and ICD10_CTRL are given
            
        If ICD9_CTRL or ICD10_CTRL or CPT_CTRL are defined, then the ccaeo/mdcro and ccaes/mdcrs files will be extracted.
        If ICD9_CTRL and ICD10_CTRL and CPT_CTRL are note defined, then the ccaet and mdcrt files will be extracted.

        """
        # If there are any conditions imposed on the control group 
        # then use that to extract the control group population
        if ((len(self.ICD9_CTRL) != 0) | (len(self.ICD10_CTRL) != 0)  | (len(self.CPT_CTRL) != 0)):

            print("Conditions imposed on control group! Will be using the DX and CPT codes to pull data from the ccaes and mdcrs files")

            return(
                    self.extract_ip_op_claims(
                        DX_LIST = [(9,dx) for dx in self.ICD9_CTRL] + [(0,dx) for dx in self.ICD10_CTRL],
                        CPT = self.CPT_CTRL,
                        ICD9 = self.ICD9_CTRL
                        )
                    )

        # If no conditions are imposed on the control group then we're going to use the general population
        # without any restriction
        else:

            print("Conditions not imposed  on control group! Will be using the ccaet and mdcrt files (i.e. enrollment detail files)")

            if self.case_ID.empty:
                return(
                        print("case_ID attribute emtpy. Need to populate with pd.DataFrame object with ENROLID in order to not select the cases.")
                        )
            else:
                return(
                        self.extract_enroll_claims(
                            process=self.process_ctrlgrp_enroll_chunk,
                            store=True,
                            idonly=False,
                            group="control",
                            N = N
                            )
                        )

    ## }}}

    ## STEP 3 : Extract ids from enrollment claims {{{
    def extract_enroll_claims(
            self,
            process,
            cols = ["ENROLID"],
            store = True,
            idonly = True,
            group = "control",
            N = 100000
            ):

        # reset the regexpattern to take Annual Enrollment Details Table (t at the end)
        self.regexpattern = "(ccae|mdcr){1}(t){1}(\d){3}.csv.gz"

        filelist = self.ip_op_filelist(
                printfiles=False
                )

        df_fin = pd.DataFrame()

        arglist = list(
                zip(
                    filelist,
                    [self.chunksize  for x in range(len(filelist))],
                    [cols  for x in range(len(filelist))],
                    [group  for x in range(len(filelist))]
                    )
                )

        pool = mp.Pool(self.cores)
        results_obj = [pool.apply_async(process, 
            args = (
                i, 
                argtup[0],
                argtup[1],
                argtup[2],
                argtup[3]
                )
            ) for i,argtup in enumerate(arglist)
            ]

        # Don't forget to close the processors
        pool.close()

        results_fin = [r.get()[1] for r in results_obj]
        df_fin = df_fin.append(pd.concat(results_fin))

        print("Finished Extracting General Populations IDs")
        # Processing if DTEND, DTSTART are datetime64 variables
        if {"DTEND"}.issubset(cols):
            df_fin["DTEND"] = pd.to_datetime(df_fin["DTEND"])
            df_fin["DTSTART"] = pd.to_datetime(df_fin["DTSTART"])

        if group == "control":
            # Store control group ID's
            if idonly:

                df_fin = df_fin.drop_duplicates()

                if store:
                    self.control_ID = df_fin.groupby("ENROLID", as_index=False).size().reset_index()[["ENROLID"]].head(N)
                else:
                    return(
                            df_fin.groupby("ENROLID", as_index=False).size().reset_index()[["ENROLID"]].head(N)
                            )
            else:

                self.control_ID  = df_fin.groupby("ENROLID", as_index=False).size().reset_index()[["ENROLID"]].head(N)
                if store:
                    self.control_claims = self.control_ID.merge(df_fin, on="ENROLID",how="inner")
                else:
                    return(
                            self.control_ID.merge(df_fin, on="ENROLID",how="inner")
                            )
        # For case group
        else:
            if idonly:

                df_fin = df_fin.drop_duplicates()

                if store:
                    self.case_ID = df_fin.groupby("ENROLID", as_index=False).size().reset_index()[["ENROLID"]]
                else:
                    return(
                            df_fin.groupby("ENROLID", as_index=False).size().reset_index()[["ENROLID"]]
                            )
            else:

                self.case_ID = df_fin.groupby("ENROLID", as_index=False).size().reset_index()[["ENROLID"]]

                if store:
                    self.case_claims = self.case_ID.merge(df_fin, on="ENROLID",how="inner")
                else:
                    return(
                            self.case_ID.merge(df_fin, on="ENROLID",how="inner")
                            )



    ## }}}

    ## STEP 3 : Filter/Processing General Population Enrollment Files {{{
    def process_ctrlgrp_enroll_chunk(
            self,
            i, 
            data, 
            chunksize,
            cols,
            group
            ):
        """
    process_enroll_chunk: 

        Function/method to process individual chunks to extract enrollment 
        claims to get at the exact number of ID's 

    Sections in The Code

    Section 1: General Setup
    Section 2: Read Chunk of Data

    Arguments: 

        data = file path/name
        chunksize = Chunk size to extract data by
    """

        # - - - - - - - - - - - - - - - - - -
        # Section 1: General Setup
        # - - - - - - - - - - - - - - - - - -

        # Create placeholder dataframe
        df_fin = pd.DataFrame()

        # - - - - - - - - - - - - - - - - - -
        # Section 2: Read Chunk of Data
        # - - - - - - - - - - - - - - - - - -

        c = 1

        # Loop through chunks 
        for chunk in pd.read_csv(data, 
                compression="gzip",
                chunksize=chunksize,
                dtype = self.dtype
                ):

        # - - - - - - - - - - - - - - - - - -
        # Section 3: Include GENPOP
        # - - - - - - - - - - - - - - - - - -

            # - - - - - - - - - - - - - - - - - -
            # 3.1: CASE = 0
            # - - - - - - - - - - - - - - - - - -
            case0 = chunk[chunk["CASE"] == 0]
            case0 = case0[cols].drop_duplicates()
            case0["CASE"] = 0 

            # - - - - - - - - - - - - - - - - - -
            # 3.2: CASE = 1
            # - - - - - - - - - - - - - - - - - -
            case1 = chunk[(chunk["CASE"] == 1) & (~chunk["ENROLID"].isin(self.case_ID["ENROLID"].values.tolist()))]
            case1 = case1[cols].drop_duplicates()
            case1["CASE"] = 1 


            df_fin = df_fin.append(case0)
            df_fin = df_fin.append(case1)
            df_fin = df_fin.drop_duplicates()

            print("Finished Chunk #", c ," and rows added: ", df_fin.shape[0]) 
            c += 1

        # - - - - - - - - - - - - - - - - - -
        # Section 4: Return Filtered Data
        # - - - - - - - - - - - - - - - - - -
        return(i, df_fin)

    ## }}}

    ## Filter/Processing EVALPOP + GENERALPOP Enrollment Files {{{
    def process_enroll_chunk_all(
            self,
            i, 
            data, 
            chunksize,
            cols,
            group
            ):
        """
    process_enroll_chunk: 

        Function/method to process individual chunks to extract enrollment 
        claims to get at the exact number of ID's 

    Sections in The Code

    Section 1: General Setup
    Section 2: Read Chunk of Data

    Arguments: 

        data = file path/name
        chunksize = Chunk size to extract data by
    """

        # - - - - - - - - - - - - - - - - - -
        # Section 1: General Setup
        # - - - - - - - - - - - - - - - - - -

        # Create placeholder dataframe

        if group == "case" :
            self.case_ID["CASE"] = 0
            IDs = self.case_ID[["ENROLID"]]

        elif group == "control" :
            self.control_ID["CASE"] = 1
            IDs = self.control_ID[["ENROLID"]]

        elif group == "both":
            self.case_ID["CASE"] = 0
            self.control_ID["CASE"] = 1
            IDs = self.case_ID.append(self.control_ID)[["ENROLID"]]
            
        df_fin = pd.DataFrame()

        # - - - - - - - - - - - - - - - - - -
        # Section 2: Read Chunk of Data
        # - - - - - - - - - - - - - - - - - -

        c = 1

        # Loop through chunks 
        for chunk in pd.read_csv(data, 
                compression="gzip",
                chunksize=chunksize,
                dtype = self.dtype
                ):

            chunk = chunk[cols]

            # Label those with 
            if re.search('(ccae.*){1}(\d){3}', data):
                chunk["PAYER"] = "Commercial"
            else:
                chunk["PAYER"] = "Medicare"

        # - - - - - - - - - - - - - - - - - -
        # Section 3: Include GENPOP
        # - - - - - - - - - - - - - - - - - -

            df_fin = df_fin.append(IDs.merge(chunk, on="ENROLID", how="inner"))

            print("Finished Chunk #", c ," and current number of rows : ", df_fin.shape[0]) 
            c += 1

        print("Finished ONEFILE!!!", df_fin.shape[0]) 
        # - - - - - - - - - - - - - - - - - -
        # Section 4: Return Filtered Data
        # - - - - - - - - - - - - - - - - - -
        return(i, df_fin)

    ## }}}

    # STEP 4 : Extract IP/OP parameters for Population of interest {{{

    def extract_ip_op_parameters(
            self,
            group = "case"
            ):

        # reset the regexpattern to take Inpatient/Outpatient claims (s|o at the end)
        self.regexpattern = "(ccae|mdcr){1}(s|o){1}(\d){3}.csv.gz"

        # Create placeholder dataframe
        df_ip = pd.DataFrame()
        df_op = pd.DataFrame()

        if group == "case":
            if self.case_ID.empty == True :
                return(
                        print("case_ID is empty")
                        )
            IDs = self.case_ID[["ENROLID"]]
        elif group == "control":
            if self.control_ID.empty == True:
                return(
                        print("control_ID is empty")
                        )
            IDs = self.control_ID[["ENROLID"]]
        elif group == "both":
            if ((self.case_ID.empty == True) | (self.control_ID.empty == True)):
                return(
                        print("case_ID and/or control_ID is empty")
                        )
            IDs = self.case_ID[["ENROLID"]].append(self.control_ID[["ENROLID"]])
        else:
            return(
                print("CHOOSE: 'case' 'control' or 'both'")
                )

        filelist = self.ip_op_filelist(
                printfiles=False
                )

        arglist = list(
                zip(
                    filelist,
                    [self.chunksize  for x in range(len(filelist))],
                    [IDs  for x in range(len(filelist))],
                    [self.filter_columns_ip_full  for x in range(len(filelist))],
                    [self.filter_columns_op_full  for x in range(len(filelist))]
                    )
                )

        pool = mp.Pool(self.cores)
        results_obj = [pool.apply_async(self.process_ip_op_evalpop_chunk, 
            args = (
                i, 
                argtup[0],
                argtup[1],
                argtup[2],
                argtup[3],
                argtup[4]
                )
            ) for i,argtup in enumerate(arglist)
            ]

        # Don't forget to close the processors
        pool.close()

        list_of_ip_df = [r.get()[1][0] for r in results_obj]
        list_of_op_df = [r.get()[1][1] for r in results_obj]
        # df_fin = df_fin.append(pd.concat(results_fin))

        print("Finished Extracting Inpatient and Outpatient Parameters for ", group)
        print("Compiling IP and OP")
        # store results in the object itself
        self.ep_ip_claims = df_ip.append(pd.concat(list_of_ip_df))
        self.ep_op_claims = df_op.append(pd.concat(list_of_op_df))

    ## }}}

    # STEP 4 : Filter/Processing Individual IP/OP files {{{

    def process_ip_op_evalpop_chunk(
            self,
            i, 
            data, 
            chunksize,
            eval_pop,
            filter_columns_ip,
            filter_columns_op
            ):
        """
    process_ip_op_evalpop_chunk: 

        Function/method to process individual chunks to extract inpatient services
        and outpatient claims to get at the exact number of ID's 

    Sections in The Code

    Section 1: General Setup
    Section 2: Read Chunk of Data
    Section 2: Include Patients using DX and CPT

    Arguments: 

        data = file path/name
        chunksize = Chunk size to extract data by
        CPT_LIST = List of CPT codes (e.g. ["1234","124512"]
        DX_LIST_PRE = Diagnosis list pre 2015 (e.g. ['3523','32342','34342']
        DX_LIST = Diagnosis list post 2015. This has to be a list of tuples where the first item in the tuple
                    is a 0 for ICD10 and 9 for ICD9. (e.g. [(0, '3523'),(9, '32342')]
    """

        # - - - - - - - - - - - - - - - - - -
        # Section 1: General Setup
        # - - - - - - - - - - - - - - - - - -

        # Create placeholder dataframe
        df_ip = pd.DataFrame()
        df_op = pd.DataFrame()

        # - - - - - - - - - - - - - - - - - -
        # Section 2: Read Chunk of Data
        # - - - - - - - - - - - - - - - - - -

        c = 1
        # Loop through chunks 
        for chunk in pd.read_csv(data, 
                compression="gzip",
                chunksize=chunksize,
                dtype = self.dtype
                ):

        # - - - - - - - - - - - - - - - - - -
        # Section 3: Include Patients using DX and CPT
        # - - - - - - - - - - - - - - - - - -

            # For IP, columns PDX exists but OP PDX and CASE ID Does not exist
            if {"PDX"}.issubset(chunk.columns):

                # - - - - - - - - - - - - - - - - - -
                # 3.1: PRE 2015
                # - - - - - - - - - - - - - - - - - -

                # For YEAR < 2015
                if {"DXVER"}.issubset(chunk.columns):
                    chunk=chunk[filter_columns_ip + ["DXVER"]]
                    chunk=chunk.merge(eval_pop[["ENROLID"]], on = "ENROLID", how = "inner")
                else:
                    chunk=chunk[filter_columns_ip]
                    chunk=chunk.merge(eval_pop[["ENROLID"]], on = "ENROLID", how = "inner")
                    chunk["DXVER"]=9

                df_ip  = df_ip.append(chunk)

                print("Finished Chunk #", c ," and rows added: ", df_ip.shape[0]) 

            # For OP claims 
            else:

                # - - - - - - - - - - - - - - - - - -
                # 3.1: PRE 2015
                # - - - - - - - - - - - - - - - - - -

                # For YEAR < 2015
                if {"DXVER"}.issubset(chunk.columns):
                    chunk=chunk[filter_columns_op + ["DXVER"]]
                    chunk=chunk.merge(eval_pop[["ENROLID"]], on = "ENROLID", how = "inner")
                else:
                    chunk=chunk[filter_columns_op]
                    chunk=chunk.merge(eval_pop[["ENROLID"]], on = "ENROLID", how = "inner")
                    chunk["DXVER"]=9

                df_op = df_op.append(chunk)

                print("Finished Chunk #", c ," and rows added: ", df_op.shape[0]) 
                c += 1


        # - - - - - - - - - - - - - - - - - -
        # Section 4: Return Filtered Data
        # - - - - - - - - - - - - - - - - - -
        return(i, (df_ip,df_op))
    
    ## }}}

    # STEP 5 : Creating Index SVC Date Data Frame {{{

    def create_svc_date_df(
            self,
            group = "case"
            ):
        if group == "case":

            ## Create DX Variables and LIST of ICD9 and ICD10
            DX_VAR = ("PDX","DX1","DX2","DX3","DX4")
            DX_LIST = [(9,dx) for dx in self.ICD9] + [(0,dx) for dx in self.ICD10]
            Diag_date_10 = pd.DataFrame()
            Diag_date_9 = pd.DataFrame()
            # pop_ID = self.case_ID[["ENROLID"]].append(self.control_ID[["ENROLID"]])
            ## Convert SVCDATE to time variables

            all_var = self.ep_ip_claims.append(self.ep_op_claims)
            all_var["DXVER"] = all_var["DXVER"].fillna(9) # fill missing DXVER with 9 
            all_var["SVCDATE"] = pd.to_datetime(all_var["SVCDATE"])
            all_var = self.case_ID.merge(all_var, on = "ENROLID")
            print(all_var.shape)
            print("Converting SVCDATE Times to datetime")

            # Loop through the IP/OP claims and extract SVC dates for Case people
            print("Extracting ICD10 DATA FOR CASES")
            i = 0
            for x in DX_VAR:
                for j in range(len(DX_LIST)):
                    if DX_LIST[j][0] in list([0]):
                        Diag_date_10 = Diag_date_10.append(
                            all_var[["ENROLID","DXVER",DX_VAR[i],"SVCDATE"]][(all_var["DXVER"]==0) & (all_var[DX_VAR[i]] == DX_LIST[j][1])], sort= True
                            )
                    j += 1
                i += 1
            print(Diag_date_10.shape)

            print("Extracting ICD9 DATA FOR CASES")
            i = 0
            for x in DX_VAR:
                for j in range(len(DX_LIST)):
                    if DX_LIST[j][0] in list([9]):
                        Diag_date_9 = Diag_date_9.append(
                            all_var[["ENROLID","DXVER",DX_VAR[i],"SVCDATE"]][(all_var["DXVER"]==9) & (all_var[DX_VAR[i]] == DX_LIST[j][1])], sort= True
                            )
                    j += 1
                i += 1
            print(Diag_date_9.shape)


            Diag_10_index=Diag_date_10[["ENROLID","SVCDATE"]].sort_values("SVCDATE").groupby("ENROLID",as_index=False).first().set_index("ENROLID").rename(columns={"SVCDATE":"SVCDATE_10"})
            Diag_9_index=Diag_date_9[["ENROLID","SVCDATE"]].sort_values("SVCDATE").groupby("ENROLID",as_index=False).first().set_index("ENROLID").rename(columns={"SVCDATE":"SVCDATE_9"})
            ENROLID_DT_CASE=pd.concat([Diag_10_index, Diag_9_index], axis=1)
            ENROLID_DT_CASE["SVCDATE_INDEX"]=ENROLID_DT_CASE.apply(lambda row: row.SVCDATE_10 if (row.SVCDATE_9>=row.SVCDATE_10)|(pd.isnull(row.SVCDATE_9)) else row.SVCDATE_9, axis=1)

            print("Finished Extracting Index Date for CASES")
            return(ENROLID_DT_CASE)

        elif group == "control": 

            print("MERGING IP and OP DATA")
            all_var = self.ep_ip_claims.append(self.ep_op_claims)
            all_var = self.control_ID.merge(all_var, on = "ENROLID", how = "inner")

            # Subset the IP/OP claims and extract SVC dates for control people
            print("Extracting ICD10 and 9 DATA FOR CONTROLS")
            Diag_date_10 = all_var[all_var["DXVER"] == 0]
            Diag_date_9 = all_var[all_var["DXVER"] == 9]
            Diag_10_index=Diag_date_10[["ENROLID","SVCDATE"]].sort_values("SVCDATE").groupby("ENROLID",as_index=False).first().set_index("ENROLID").rename(columns={"SVCDATE":"SVCDATE_10"})
            Diag_9_index=Diag_date_9[["ENROLID","SVCDATE"]].sort_values("SVCDATE").groupby("ENROLID",as_index=False).first().set_index("ENROLID").rename(columns={"SVCDATE":"SVCDATE_9"})
            ENROLID_DT_CTRL=pd.concat([Diag_10_index, Diag_9_index], axis=1)
            ENROLID_DT_CTRL["SVCDATE_INDEX"]=ENROLID_DT_CTRL.apply(lambda row: row.SVCDATE_10 if (row.SVCDATE_9>=row.SVCDATE_10)|(pd.isnull(row.SVCDATE_9)) else row.SVCDATE_9, axis=1)

            print("Finished Extracting Index Date for CONTROLS")
            return(ENROLID_DT_CTRL)

        elif group == "both": 

            ## Create DX Variables and LIST of ICD9 and ICD10
            DX_VAR = ("PDX","DX1","DX2","DX3","DX4")
            DX_LIST = [(9,dx) for dx in self.ICD9] + [(0,dx) for dx in self.ICD10]
            Diag_date_10 = pd.DataFrame()
            Diag_date_9 = pd.DataFrame()
            # pop_ID = self.case_ID[["ENROLID"]].append(self.control_ID[["ENROLID"]])
            ## Convert SVCDATE to time variables
            print("Converting SVCDATE Times to datetime")
            self.ep_ip_claims["SVCDATE"] = pd.to_datetime(self.ep_ip_claims["SVCDATE"])
            self.ep_op_claims["SVCDATE"] = pd.to_datetime(self.ep_op_claims["SVCDATE"])

            all_var = self.ep_ip_claims.append(self.ep_op_claims)
            all_var = self.case_ID.merge(all_var, on = "ENROLID", how = "inner")

            # Loop through the IP/OP claims and extract SVC dates for Case people
            print("Extracting ICD10 DATA FOR CASES")
            i = 0
            for x in DX_VAR:
                for j in range(len(DX_LIST)):
                    if DX_LIST[j][0] in list([0]):
                         Diag_date_10 = Diag_date_10.append(
                            all_var[["ENROLID","DXVER",DX_VAR[i],"SVCDATE"]][(all_var["DXVER"]==0) & (all_var[DX_VAR[i]] == DX_LIST[j][1])], sort= True
                            )
                    j += 1
                i += 1

            print("Extracting ICD9 DATA FOR CASES")
            i = 0
            for x in DX_VAR:
                for j in range(len(DX_LIST)):
                    if DX_LIST[j][0] in list([9]):
                        Diag_date_9 = Diag_date_9.append(
                            all_var[["ENROLID","DXVER",DX_VAR[i],"SVCDATE"]][(all_var["DXVER"]==9) & (all_var[DX_VAR[i]] == DX_LIST[j][1])], sort= True
                            )
                    j += 1
                i += 1


            Diag_10_index=Diag_date_10[["ENROLID","SVCDATE"]].sort_values("SVCDATE").groupby("ENROLID",as_index=False).first().set_index("ENROLID").rename(columns={"SVCDATE":"SVCDATE_10"})
            Diag_9_index=Diag_date_9[["ENROLID","SVCDATE"]].sort_values("SVCDATE").groupby("ENROLID",as_index=False).first().set_index("ENROLID").rename(columns={"SVCDATE":"SVCDATE_9"})
            ENROLID_DT_CASE=pd.concat([Diag_10_index, Diag_9_index], axis=1)
            ENROLID_DT_CASE["SVCDATE_INDEX"]=ENROLID_DT_CASE.apply(lambda row: row.SVCDATE_10 if (row.SVCDATE_9>=row.SVCDATE_10)|(pd.isnull(row.SVCDATE_9)) else row.SVCDATE_9, axis=1)

            print("Finished Extracting Index Date for CASES")

            # CONTROLS
            all_var = self.ep_ip_claims.append(self.ep_op_claims)
            all_var = self.control_ID.merge(all_var, on = "ENROLID", how = "inner")

            # Subset the IP/OP claims and extract SVC dates for control people
            print("Extracting ICD10 and 9 DATA FOR CONTROLS")
            Diag_date_10 = all_var[all_var["DXVER"] == 0]
            Diag_date_9 = all_var[all_var["DXVER"] == 9]
            Diag_10_index=Diag_date_10[["ENROLID","SVCDATE"]].sort_values("SVCDATE").groupby("ENROLID",as_index=False).first().set_index("ENROLID").rename(columns={"SVCDATE":"SVCDATE_10"})
            Diag_9_index=Diag_date_9[["ENROLID","SVCDATE"]].sort_values("SVCDATE").groupby("ENROLID",as_index=False).first().set_index("ENROLID").rename(columns={"SVCDATE":"SVCDATE_9"})
            ENROLID_DT_CTRL=pd.concat([Diag_10_index, Diag_9_index], axis=1)
            ENROLID_DT_CTRL["SVCDATE_INDEX"]=ENROLID_DT_CTRL.apply(lambda row: row.SVCDATE_10 if (row.SVCDATE_9>=row.SVCDATE_10)|(pd.isnull(row.SVCDATE_9)) else row.SVCDATE_9, axis=1)

            ENROLID_DT=ENROLID_DT_CASE.append(ENROLID_DT_CTRL)
            print("Finished Extracting Index Date for CONTROLS")

            return(ENROLID_DT)

        else:
            return(
                    print("INVALID RESPONES: Choose case, control or both")
                    )
    ## }}}

    # STEP 6 : Extract Evaluable Population by applying continuous enrollment Criteria {{{

    ## UTILITY FUNCTIONS {{{
    # UTILITY FUNCTION 1
    def label_DTSTART(self, row):
        if pd.isnull(row["DTSTART_y"]) == True:
            return row["DTSTART_x"]
        else:
            return row["DTSTART_y"]

    ### UTILITY FUNCTION 2- Condition 1: Individuals must have at least the same number of rows if not then they just don't have enough enrollment data so not continuous enrolled 
    def CE_condition1(self, x, months):
        if x.shape[0] < months:
            return False
        else:
            return True

    ### UTILITY FUNCTION 3- Condition 3: SUM OF MEMDAYS + 45 has to be >= the continuous enrollment criteria specified in terms of months
    def CE_condition3(self,x,months):
        if sum(x["MEMDAYS_new"]) + 45 >= 365.24/12 * months:
            return True
        else:
            return False

    ### UTILITY FUNCTION 4- replacement
    def Replace_DTSTART(self,x):
        if x["DTEND_x"] == x["DTEND_y"]:
            return x["DTSTART_y"]
        else: 
            return x["DTSTART_x"]

    ### UTILITY FUNCTION 5- make chunks 
    def mkchunks(self, l, n):
        for i in range(0, len(l), n):
            yield l[i:i+n]


    ## }}}

    ## MAIN FUNCTIONS {{{

    ### MERGE IDX AND ENROLLMENT DATA FRAMES FOR ANALYSIS {{{
    def Merge_IDX_ENR(self, 
            INDEX_DF, 
            ENROL_DF
            ):

        ## 1. MERGING DATA TOGETHER and SETTING INDEX DATE on DTSTART and RECALCULATING MEMDAYS_new {{{
        ## Merge index dates with enrollment df
        temp = INDEX_DF.reset_index()[["ENROLID","SVCDATE_INDEX"]].merge(ENROL_DF)

        ## Convert DTSTART and DTEND variables to datetime object
        temp["DTSTART"] = pd.to_datetime(temp["DTSTART"]) 
        temp["DTEND"] = pd.to_datetime(temp["DTEND"]) 
        temp["SVCDATE_INDEX"] = pd.to_datetime(temp["SVCDATE_INDEX"]) 

        ### Identify DTEND's that are geq than Index Date for each ENROLID
        INDEX_DF1 = temp.groupby("ENROLID").apply(lambda row: row[row.SVCDATE_INDEX<=row.DTEND]).set_index("ENROLID").sort_values("DTEND").groupby("ENROLID").first().reset_index()

        ### Change the DTSTART to SVCDATE_INDEX
        INDEX_DF1['DTSTART'] = INDEX_DF1['SVCDATE_INDEX']

        ### Merge back the one row for each of the ENROLID
        NEWENROL_DF = temp.merge(INDEX_DF1[["ENROLID","DTSTART","DTEND"]], on=["ENROLID","DTEND"], how="left")

        ### CHANGE DTSTART to The SVCDATE_INDEX 
        NEWENROL_DF["DTSTART"] = NEWENROL_DF.apply(lambda row: self.label_DTSTART(row), axis=1)

        ### RECALCULATE MEMDAYS
        NEWENROL_DF["MEMDAYS_new"] = NEWENROL_DF["DTEND"] - NEWENROL_DF["DTSTART"]
        NEWENROL_DF["MEMDAYS_new"] = NEWENROL_DF.MEMDAYS_new.dt.days
        ## }}}

        return(NEWENROL_DF)
    ### }}}

    ### RECURSIVELY EXTRACT CONTINUOUSLY ENROLLED IDS {{{
    def Recursive_Extract_CI_IDs(self,
            NEWENROL_DF,
            ENROL_FIN, # a empty pd.DataFrame() needs to be an input as recursively it goes in and stuff gets added
            repeat = 0, # how many times this function has been called
            strict = True
            ):
        """
        Recursive_Extract_CI_IDs(): Function to identify continuously enrolled participants by chunking the enrollment
                                    data for each individuals in the NEWENROL_DF file by 12 months. The `NEWENROL_DF` 
                                    should be processed from `Merge_IDX_ENR()` such that the index date serves as the
                                    first start date of the enrollment period. This method implies continuous enrollment
                                    Starting from the index date

        Arguments:

            NEWENROL_DF =   pd.DataFrame() cleaned and processed using `Merge_IDX_ENR()`
            ENROL_FIN =     Empty pd.DataFrame() needed to be instantiated outside of the function
            repeat =        How many times the function has chunked through the data. Each repeat represents year - 1.
            strict =        If strict == True, participants who fail to meet continuous enrollment starting from the 1st 

        """

        # import pdb
        # pdb.set_trace()

        print("Checking Patients with ", repeat+1, " year continuous enrollment ")
        print("Current size of the enrollment data: ", NEWENROL_DF.shape)
        # If we haven't repeated the function enough to output continuous enrollment data... continue on
        # if NEWENROL_DF.empty == True:
        if 12 > int(NEWENROL_DF[["ENROLID","DTSTART"]].groupby("ENROLID").agg(["count"]).max()) :
            print("FINISHED COMPILING CE DATA!!! ")
            print("Final CE data: ", self.CE_df.groupby("CE").size())

        else:
        # if 12 <= int(NEWENROL_DF[["ENROLID","DTSTART"]].groupby("ENROLID").agg(["count"]).max()) :

            ### Sort by DTEND ASCENDING (from beginning to end) and select 12 rows
            # ENROL = NEWENROL_DF.groupby("ENROLID").apply(lambda id: id.sort_values(["DTEND"])).reset_index(drop=True).groupby("ENROLID").head(12)
            ENROL = NEWENROL_DF.groupby("ENROLID").head(12).groupby("ENROLID").apply(lambda id: id.sort_values(["DTEND"])).reset_index(drop=True)
            ENROL_WIDE = pd.DataFrame()
            # IMPOSE CONDITION 1: The number of rows for individuals is at least as much as the `months` argument 
            ENROL_WIDE["CE1"] = ENROL.groupby("ENROLID").apply(lambda x: self.CE_condition1(x,12))
            # IMPOSE CONDITION 2: The range of the enrollment period in days has less than 45 days difference
            ENROL_WIDE["CE2"] = np.abs(((ENROL.groupby("ENROLID").last().DTEND - ENROL.groupby("ENROLID").first().DTSTART) - pd.Timedelta(pd.Timedelta(str(365.24/12 * 12) + " days"))).dt.days) <= 45
            # IMPOSE CONDITION 3: The absolute value of the difference between Sum of the memberdays and the CE month is <= 45 days
            ENROL_WIDE["CE3"] = ENROL.groupby("ENROLID").apply(lambda x: self.CE_condition3(x,12))

            ENROL_EXCL = ENROL_WIDE[((ENROL_WIDE["CE1"] != True) | (ENROL_WIDE["CE2"] != True)  | (ENROL_WIDE["CE3"] != True))],
            ENROL_EXCL = ENROL_EXCL[0]
            ENROL_EXCL["CE"] = repeat
            repeat += 1
            ENROL_INCL = ENROL_WIDE[((ENROL_WIDE["CE1"] == True) & (ENROL_WIDE["CE2"] == True)  & (ENROL_WIDE["CE3"] == True))],
            ENROL_INCL = ENROL_INCL[0]
            ENROL_INCL["CE"] = repeat

            # FILTER THE NEWENROL_DF that has been included so that we get the next chunk of months
            NEWENROL_ALL = NEWENROL_DF.merge(ENROL[["ENROLID","DTEND"]], on=["ENROLID","DTEND"], how="left", indicator=True)
            NEWENROL_DF = NEWENROL_ALL[NEWENROL_ALL['_merge'] == 'left_only']
            NEWENROL_DF = NEWENROL_DF[["ENROLID","DTSTART","DTEND","MEMDAYS_new"]]

            # Append IDs with CE status
            ENROL_FIN = ENROL_FIN.append(ENROL_INCL.append(ENROL_EXCL).reset_index()[["ENROLID","CE"]])
            # Remove the duplicates
            ENROL_FIN = ENROL_FIN.groupby("ENROLID").apply(lambda id: id.sort_values(["CE"], ascending=False)).reset_index(drop=True).groupby("ENROLID").head(1)
            print("CE Groups: ", ENROL_FIN.groupby("CE").size())
            # ENROL_FIN = ENROL_FIN.groupby("ENROLID").apply(lambda id: id.sort_values(["CE"])).groupby("ENROLID").head(1)
            # ENROL_FIN = ENROL_FIN.groupby("ENROLID").apply(lambda id: id.sort_values(["CE"])).groupby("ENROLID").head(1)
            self.CE_df = ENROL_FIN

            # FILTER ROWS FROM INDIVIDUALS THAT HAVE BEEN EXCLUDED
            # THE NEWENROL_DF will have fewer observations than the overall due to getting rid of the excluded patients' observations
            if strict == True:
                ENROL_EXCL = ENROL_EXCL.reset_index()[["ENROLID"]].merge(NEWENROL_DF, how="inner")
                NEWENROL_ALL = NEWENROL_DF.merge(ENROL_EXCL[["ENROLID","DTEND"]], on=["ENROLID","DTEND"], how="left", indicator=True)
                NEWENROL_DF = NEWENROL_ALL[NEWENROL_ALL['_merge'] == 'left_only']
                NEWENROL_DF = NEWENROL_DF[["ENROLID","DTSTART","DTEND","MEMDAYS_new"]]

            # MERGE LAST ROW OF EACH INCLUDED INDIVIDALs ENROLLMENT CHUNK where DTEND becomes DTSTART of the the first row in the next chunk
            ENROL_INCL = ENROL_INCL.reset_index()[["ENROLID"]].merge(ENROL[["ENROLID","DTSTART","DTEND","MEMDAYS_new"]], how="inner")
            ENROL_INCL = ENROL_INCL.groupby("ENROLID").last().reset_index()
            ENROL_INCL["DTSTART"] = ENROL_INCL["DTEND"]
            ENROL_INCL = ENROL_INCL[["ENROLID","DTSTART"]]


            NEWENROL_DF_ROWREPLACE = NEWENROL_DF.groupby("ENROLID").head(1).merge(ENROL_INCL, on = "ENROLID", how = "inner")
            NEWENROL_DF_ROWREPLACE["DTSTART"] = NEWENROL_DF_ROWREPLACE["DTSTART_y"]
            NEWENROL_DF_ROWREPLACE= NEWENROL_DF_ROWREPLACE[["ENROLID","DTSTART","DTEND"]]
            NEWENROL_DF_ROWREPLACE["MEMDAYS_new"] = NEWENROL_DF_ROWREPLACE["DTEND"] - NEWENROL_DF_ROWREPLACE["DTSTART"]
            NEWENROL_DF_ROWREPLACE["MEMDAYS_new"] = NEWENROL_DF_ROWREPLACE.MEMDAYS_new.dt.days
            NEWENROL_DF = NEWENROL_DF.merge(NEWENROL_DF_ROWREPLACE, on=["ENROLID"], how="left")

            try:
                if 12 <= int(NEWENROL_DF[["ENROLID","DTSTART_x"]].groupby("ENROLID").agg(["count"]).max()) :
                    NEWENROL_DF["DTSTART"] = NEWENROL_DF.apply(lambda x: self.Replace_DTSTART(x), axis=1)
                    NEWENROL_DF["DTEND"] = NEWENROL_DF["DTEND_x"]

                    # RECALCULATE MEMDAYS_new after merging
                    NEWENROL_DF["MEMDAYS_new"] = NEWENROL_DF["DTEND"] - NEWENROL_DF["DTSTART"]
                    NEWENROL_DF["MEMDAYS_new"] = NEWENROL_DF.MEMDAYS_new.dt.days
                    NEWENROL_DF = NEWENROL_DF[["ENROLID","DTSTART","DTEND","MEMDAYS_new"]]

                    self.Recursive_Extract_CI_IDs(
                            NEWENROL_DF=NEWENROL_DF,
                            ENROL_FIN=ENROL_FIN,
                            repeat=repeat
                            )
            except:
                return(print("END OF COMPILATION!"))

    ### }}}

    ### Extract evaluation population based on years {{{
    def extract_eval_pop(
            self,
            years=1
            ):

        if self.CE_df.empty == True:
            print("CE_df is empty! Add the continuous enrollment data")

        else:
            return(self.CE_df[self.CE_df["CE"] >= years])

    # }}}

    ### Extract Continuously Enrolled Patients using ENROL_DF {{{

    def Extract_CE_IDs(
            self,
            ENROL_DF
            ):

        """
        Extract_CI_IDs():   Function to identify continuously enrolled participants by chunking the enrollment
                            data for each individuals in the ENROL_DF file by 12 months. The `ENROL_DF` 
                            can be processed from `Merge_IDX_ENR()` such that the index date serves as the
                            first start date of the enrollment period or it may be left using the general enrollment
                            starting period from the ccaet|mdcrt file. This method will identify the period for each 
                            individual that has the maximum amount of continuous enrollment (CE) time captured.
                            For example, an individual may have 1 year CE then a gap of 6 months then 2 years of CE afterwards.
                            This individual will be considered having 2 years CE because that is the period that is the
                            maximum CE period the person has.

        Arguments:

            ENROL_FIN =     Enrollment summary dataframe that contains all enrollment information based on the ccaet|mdcrt
                            claims file. It should be a pd.DataFrame() 

        """

        df_fin = pd.DataFrame()
        IDs = ENROL_DF.drop_duplicates(subset="ENROLID")[["ENROLID"]]
        # Make ID's into chunk so that we can process it in parallel to determine continuous enrollment
        ID_chunk = list(
                self.mkchunks(
            IDs.ENROLID.values.tolist(), int((len(IDs.ENROLID.values.tolist()))/self.cores)
            )
                )

        arglist = list(
                zip(
                    ID_chunk,
                    [ENROL_DF[["ENROLID","DTSTART","DTEND"]]  for x in range(len(ID_chunk))]
                    )
                )

        # Init parallel processes
        pool = mp.Pool(self.cores)
        results_obj = [pool.apply_async(self.process_extract_ce_ids, 
            args = (
                i, 
                argtup[0],
                argtup[1]
                )
            ) for i,argtup in enumerate(arglist)
            ]
        # Don't forget to close the processors
        pool.close()
        results_fin = [r.get()[1] for r in results_obj]
        df_fin = df_fin.append(pd.concat(results_fin))

        # Since sometimes the ID's come out as floats...  reconvert to INT
        df_fin["ENROLID"] = [ int(x) for x in df_fin.ENROLID.values.tolist()]

        # return results 

        print("FINISHED COMPILING ID DF of CONTINUOUSLY ENROLLED PARTICIPANTS")
        return(df_fin)

    ### }}}

    ### Extract Continuous Enrolled Participants Parallel Chunk Function {{{
    def process_extract_ce_ids(
            self,
            i, 
            IDs, 
            ENROL_DF
            ):
        """
    process_ip_op_chunk: 

        Function/method to process individual chunks to extract continuously enrolled participants  

    Sections in The Code

    Section 1: General Setup
    Section 2: Read Chunk of Data
    Section 2: Include Patients using DX and CPT

    Arguments: 

        data = file path/name
    """

        ## SETUP THE STUDY VARIABLES
        ## CONTINUOUS ENROLLMENT MONTHS
        ce_ind_df = pd.DataFrame()
        # CONVERT DATES
        ENROL_DF["DTSTART"] = pd.to_datetime(ENROL_DF["DTSTART"])
        ENROL_DF["DTEND"] = pd.to_datetime(ENROL_DF["DTEND"])

        # import pdb
        # pdb.set_trace()
        j = 0
        for ind in IDs:
            # print("ASSESSING INDIVIDUAL: ", ind, " FOR CONTINUOUS ENROLLMENT.")
            # set indicator for individual
            indinc = 0
            # Load whole individuals data
            ind_df = ENROL_DF[ENROL_DF["ENROLID"] == ind].reset_index(drop=True)
            ind_df = ind_df[["ENROLID","DTSTART","DTEND"]]
            covgap = 0
            # If inidvidual doesn't have more than 11 rows then skip
            if ind_df.shape[0] < 11:
                # print("INDIVIDUAL: ", ind, " HASS LESS THAN 12 TOTAL RECORDS... NOT CONTINUOUSLY ENROLLED.")
                ce_ind_df.loc[j,"ENROLID"] = ind
                ce_ind_df.loc[j,"CE"] = 0
                pass
            else:
                # For each of the months being used to check continuous enrollment
                # for month in range(0, min(study_period - months + 2,ind_df.shape[0]-1)):
                for month in range(0, ind_df.shape[0]-12+2):
                    # print("MONTH : ", month, " FOR INDIVIDUAL ", ind)
                    # create a yearly data so that 45 days allowable gap per year can be assessed

                    covgap_sum = 0
                    yearly_df = pd.DataFrame()
                    yearce = 0
                    for year in range(0, int(np.floor(ind_df.shape[0]/12))):
                        covgap_sum = 0
                        yearly_df = ind_df.loc[((12*year)+month):(((12*(year+1)) - 1)+month),:]
                        yearly_df = yearly_df[~yearly_df.isna()].reset_index(drop=True)
                        # If the data frame that's created is less than 12 rows then this person
                        if yearly_df.shape[0] < 11:
                            # print("INDIVIDUAL: ", ind, " ONLY HAS ", yearly_df.shape[0], " ROWS!")
                            # ce_ind_df.loc[j,"ENROLID"] = ind
                            # ce_ind_df.loc[j,"CE"] = year
                            break

                        # Add coverage gap based on LAST DTEND and FIRST DTSTART
                        covgap_sum += (pd.Timedelta("365 days") - (yearly_df.loc[yearly_df.shape[0] - 1,"DTEND"] - yearly_df.loc[0,"DTSTART"] + pd.Timedelta("1 day"))).days

                        for month_in_yr in range(0, yearly_df.shape[0]-1):
                            covgap_sum += (yearly_df.loc[month_in_yr+1, "DTSTART"] - yearly_df.loc[month_in_yr, "DTEND"] - pd.Timedelta("1 day")).days
                            if covgap_sum > 45:
                                break

                        if covgap_sum > 45:
                            ce_ind_df.loc[j,"ENROLID"] = ind
                            ce_ind_df.loc[j,"CE"] = year
                            break

                        # if (year == int(np.floor(months/12))) & (covgap_sum < 45*int(np.floor(months/12))):
                        if (year == int(np.floor(ind_df.shape[0]/12)) - 1) & (covgap_sum <= 45):
                            # print("CONTINUOUSLY ENROLLED INDIVIDUAL : ", ind)
                            ce_ind_df.loc[j,"ENROLID"] = ind
                            ce_ind_df.loc[j,"CE"] = year + 1
                            indinc = 1
                            break

                        yearce += 1

                    if yearly_df.shape[0] < 12:
                        ce_ind_df.loc[j,"ENROLID"] = ind
                        ce_ind_df.loc[j,"CE"] = yearce
                        break

                    if indinc == 1:
                        break

                # if indinc == 1:
                    # break

            # Change individuals
            j += 1
            # print("ASSESSING INDIVIDUAL : ", ind, " COMPLETE.")
            if j == int(0.1*len(IDs)):
                print("10% COMPLETE ")
            if j == int(0.2*len(IDs)):
                print("20% COMPLETE ")
            if j == int(0.3*len(IDs)):
                print("30% COMPLETE ")
            if j == int(0.4*len(IDs)):
                print("40% COMPLETE ")
            if j == int(0.5*len(IDs)):
                print("50% COMPLETE ")
            if j == int(0.6*len(IDs)):
                print("60% COMPLETE ")
            if j == int(0.7*len(IDs)):
                print("70% COMPLETE ")
            if j == int(0.8*len(IDs)):
                print("80% COMPLETE ")
            if j == int(0.9*len(IDs)):
                print("90% COMPLETE ")
            if j == int(1*len(IDs)):
                print("100% COMPLETE ")

        return(i, ce_ind_df)

    ### }}}

    ### PRINT CE POP  {{{

    def print_ce_pop(
            self
            ):
        """
        print_ce_pop():     Print method to print participants that are continuously enrolled for at least the duration listed
                            in the "CE" column.
        """

        if self.CE_df.empty == True:
            return(
                    print("self.CE_df is empty! populate with the CE extracted pd.DataFrame()")
                    )

        temp = self.CE_df.groupby("CE", as_index=True).size().reset_index()

        temp = pd.DataFrame({
            "YEARS": temp.CE.values.tolist(),
            "CE_POP": [
                int(temp[temp["CE"] >= 0].agg("sum").values.tolist()[1]),
                int(temp[temp["CE"] >= 1].agg("sum").values.tolist()[1]),
                int(temp[temp["CE"] >= 2].agg("sum").values.tolist()[1]),
                int(temp[temp["CE"] >= 3].agg("sum").values.tolist()[1]),
                int(temp[temp["CE"] >= 4].agg("sum").values.tolist()[1]),
                int(temp[temp["CE"] >= 5].agg("sum").values.tolist()[1])
                ]
            })


        print(
                print(temp)
                )
        ## }}}

    ## }}}

    # }}}

    # STEP 7 : Extract Demographic Variables and CCI information {{{

    def extract_demo(
            self,
            group = "case"
            ):

        # Change thte regular expression to look for the ccaea or mdcra files
        self.regexpattern = "(ccae|mdcr){1}(a){1}(\d){3}.csv.gz"
        filelist = self.ip_op_filelist(
                printfiles=False
                )
        if len(filelist) == 0:
            return(print("No files to append! Check the directory spelling!"))

        df_fin = pd.DataFrame()

        if group == "case":

            arglist = list(
                    zip(
                        filelist,
                        [self.chunksize  for x in range(len(filelist))],
                        [self.filter_columns_demo for x in range(len(filelist))],
                        [group for x in range(len(filelist))]
                        )
                    )

        elif group == "control":

            arglist = list(
                    zip(
                        filelist,
                        [self.chunksize  for x in range(len(filelist))],
                        [self.filter_columns_demo for x in range(len(filelist))],
                        [group for x in range(len(filelist))]
                        )
                    )

        elif group == "both":

            arglist = list(
                    zip(
                        filelist,
                        [self.chunksize  for x in range(len(filelist))],
                        [self.filter_columns_demo for x in range(len(filelist))],
                        [group for x in range(len(filelist))]
                        )
                    )

        else:
            return(
                    print("Options for extracting claims: 'case' or 'control' or 'both'")
                    )


        # Init parallel processes
        pool = mp.Pool(self.cores)
        results_obj = [pool.apply_async(self.process_enroll_chunk_all, 
            args = (
                i, 
                argtup[0],
                argtup[1],
                argtup[2],
                argtup[3]
                )
            ) for i,argtup in enumerate(arglist)
            ]
        # Don't forget to close the processors
        pool.close()
        results_fin = [r.get()[1] for r in results_obj]
        df_fin = df_fin.append(pd.concat(results_fin))

        # return results 

        print("FINISHED COMPILING ", group, " claims!")
        print("Sorting Duplicate Enrollment Claims... Taking the earliest Year!")
        print("Results are demographic claims data of ", group, " group ! It's stored in .demo_df")

        self.demo_df = df_fin.sort_values(["ENROLID","YEAR"]).groupby("ENROLID").head(1)


    # }}}

# }}}


