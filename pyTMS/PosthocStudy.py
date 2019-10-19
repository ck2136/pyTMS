#!/usr/bin/env python
# - - - - - - - - - - - - - - - - - - - - - # 
# Filename      : PosthocStudy.py
# Purpose       : PosthocStudy class
# Date created  : Wed 16 Oct 2019 10:15:07 AM MDT
# Created by    : ck
# Last modified : Wed 16 Oct 2019 10:15:18 AM MDT
# Modified by   : ck
# - - - - - - - - - - - - - - - - - - - - - # 

class PosthocStudy(Study):

    """
    PosthocStudy Description
    """

    def __init__(
            self,
            pop_ID, 
            pop_claims,
            ICD9,
            ICD10,
            CPT
            ):
        """TODO: to be defined. """
        Study.__init__(
                name = "Default Posthoc Study Name",
                pop_ID, 
                pop_claims,
                ICD9,
                ICD10,
                CPT
                ):


        
