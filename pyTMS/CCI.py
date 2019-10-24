#!/usr/bin/env python
# - - - - - - - - - - - - - - - - - - - - - # 
# Filename      : CCI.py
# Purpose       : CCI class file
# Date created  : Sun 20 Oct 2019 12:32:48 PM MDT
# Created by    : ck
# Last modified : Thu 24 Oct 2019 07:16:23 AM MDT
# Modified by   : ck
# - - - - - - - - - - - - - - - - - - - - - # 

class CCI(object):

    """Chronic Comorbidity Index Class"""

    def __init__(self):
        """
        CCI Class has multiple attributes to help identifying CCI categories for individuals 
        """

        # ICD10 Groups according to Quan et al. (2005)
        self.CCI_NAME = ("CCI_MI", "CCI_CHF", "CCI_PVD", "CCI_CD", "CCI_DEM", "CCI_CPD", "CCI_RHEUM", 
                    "CCI_PUD", "CCI_MLD", "CCI_DIAB_NO_COMP", "CCI_DIAB_W_COMP", "CCI_PLEG", 
                    "CCI_RENAL", "CCI_MALIG", "CCI_LD", "CCI_SOLID_TUMOR", "CCI_HIV")
        self.CCI_MI_10 = (["I21", "I22"], ["I252"])
        self.CCI_CHF_10 = (["I43", "I50"], ["I099", "I110", "I130", "I132", "I255", "I420", "I425", "I426", "I427", "I428", "I429", "P290"])
        self.CCI_PVD_10 = (["I70", "I71"], ["I731", "I738", "I739", "I771", "I790", "I792", "K551", "K558", "K559", "Z958", "Z959"])
        self.CCI_CD_10 = (["G45", "G46", "I60", "I61", "I62", "I63", "I64", "I65", "I66", "I67", "I68", "I69"], ["H340"])
        self.CCI_DEM_10 = (["F00", "F01", "F02", "F03", "G30"], ["F051", "G311"])
        self.CCI_CPD_10 = (["J40", "J41", "J42", "J43", "J44", "J45", "J46", "J47", "J60", "J61", "J62", "J63", "J64", "J65", "J66", "J67"], ["I278", "I279", "J684", "J701", "J703"])
        self.CCI_RHEUM_10 = (["M05", "M06", "M32", "M33", "M34"], ["M315", "M351", "M353", "M360"])
        self.CCI_PUD_10 = (["K25", "K26", "K27", "K28"], [])
        self.CCI_MLD_10 = (["B18", "K73", "K74"], ["K700", "K701", "K702", "K703", "K709", "K713", "K714", "K715", "K717", "K760", "K762", "K763", "K764", "K768", "K769", "Z944"])
        self.CCI_DIAB_NO_COMP_10 = ([], ["E100", "E101", "E106", "E108", "E109", "E110", "E111", "E116", "E118", "E119", "E120", "E121", "E126", "E128", "E129", "E130", "E131", "E136", "E138", "E139", "E140", "E141", "E146", "E148", "E149"])
        self.CCI_DIAB_W_COMP_10 = ([], ["E102", "E103", "E104", "E105", "E107", "E112", "E113", "E114", "E115", "E117", "E122", "E123", "E124", "E125", "E127", "E132", "E133", "E134", "E135", "E137", "E142", "E143", "E144", "E145", "E147"])
        self.CCI_PLEG_10 = (["G81", "G82"], ["G041", "G114", "G801", "G802", "G830", "G831", "G832", "G833", "G834", "G839"])
        self.CCI_RENAL_10 = (["N18", "N19"], ["I120", "I131", "N032", "N033", "N034", "N035", "N036", "N037", "N052", "N053", "N054", "N055", "N056", "N057", "N250", "Z490", "Z491", "Z492", "Z940", "Z992"])
        self.CCI_MALIG_10 = (["C00", "C01", "C02", "C03", "C04", "C05", "C06", "C07", "C08", "C09", "C10", "C11", "C12", "C13", "C14", "C15", "C16",
                           "C17", "C18", "C19", "C20", "C21", "C22", "C23", "C24", "C25", "C26", "C30", "C31", "C32", "C33", "C34", "C37", "C38",
                           "C39", "C40", "C41", "C43", "C45", "C46", "C47", "C48", "C49", "C50", "C51", "C52", "C53", "C54", "C55", "C56", "C57",
                           "C58", "C60", "C61", "C62", "C63", "C64", "C65", "C66", "C67", "C68", "C69", "C70", "C71", "C72", "C73", "C74", "C75",
                           "C76", "C81", "C82", "C83", "C84", "C85", "C88", "C90", "C91", "C92", "C93", "C94", "C95", "C96", "C97"], [])
        self.CCI_LD_10 = ([], ["I850", "I859", "I864", "I982", "K704", "K711", "K721", "K729", "K765", "K766", "K767"])
        self.CCI_SOLID_TUMOR_10 = (["C77", "C78", "C79", "C80"], [])
        self.CCI_HIV_10 = (["B20", "B21", "B22", "B24"], [])
        self.CCI_LIST_10 = (self.CCI_MI_10, self.CCI_CHF_10, self.CCI_PVD_10, self.CCI_CD_10, self.CCI_DEM_10, self.CCI_CPD_10, self.CCI_RHEUM_10,
                            self.CCI_PUD_10, self.CCI_MLD_10, self.CCI_DIAB_NO_COMP_10, self.CCI_DIAB_W_COMP_10, self.CCI_PLEG_10, 
                            self.CCI_RENAL_10, self.CCI_MALIG_10, self.CCI_LD_10, self.CCI_SOLID_TUMOR_10, self.CCI_HIV_10)

        # ICD9 Groups according to Quan et al. (2005)
        self.CCI_MI_9 = (["410", "412"], [])
        self.CCI_CHF_9 = (["428"], ["39891", "40201", "40211", "40291", "40401", "40403", "40411", "40413", "40491", "40493", "4254", "4255", "4256", "4257", "4258", "4259"])
        self.CCI_PVD_9 = (["440", "441"], ["0930", "4373", "4431", "4432", "4433", "4434", "4435", "4436", "4437", "4438", "4439", "4471", "5571", "5579", "V434"])
        self.CCI_CD_9 = (["430", "431", "432", "433", "434", "435", "436", "437", "438"], ["36234"])
        self.CCI_DEM_9 = (["290"], ["2941", "3312"])
        self.CCI_CPD_9 = (["490", "491", "492", "493", "494", "495", "496", "497", "498", "499", "500", "501", "502", "503", "504", "505"], ["4168", "4169", "5064", "5081", "5088"])
        self.CCI_RHEUM_9 = (["725"], ["4465", "7100", "7101", "7102", "7103", "7104", "7140", "7141", "7142", "7148"])
        self.CCI_PUD_9 = (["531", "532", "533", "534"], [])
        self.CCI_MLD_9 = (["570", "571"], ["07022", "07023", "07032", "07033", "07044", "07054", "0706", "0709", "5733", "5734", "5738", "5739", "V427"])
        self.CCI_DIAB_NO_COMP_9 = ([], ["2500", "2501", "2502", "2503", "2508", "2509"])
        self.CCI_DIAB_W_COMP_9 = ([], ["2504", "2505", "2506", "2507"])
        self.CCI_PLEG_9 = (["342", "343"], ["3341", "3440", "3441", "3442", "3443", "3444", "3445", "3446", "3449"])
        self.CCI_RENAL_9 = (["582", "585", "586", "V56"], ["40301", "40311", "40391", "40402", "40403", "40412", "40413", "40492", "40493", "5830", "5831", "5832", "5833", "5834", "5835", "5836", "5837", "5880", "V420", "V451"])
        self.CCI_MALIG_9 = (["140", "141", "142", "143", "144", "145", "146", "147", "148", "149", 
                           "150", "151", "152", "153", "154", "155", "156", "157", "158", "159", 
                           "160", "161", "162", "163", "164", "165", "166", "167", "168", "169",
                           "170", "171", "172", "174", "175", "176", "177", "178", "179", 
                           "180", "181", "182", "183", "184", "185", "186", "187", "188", "189",
                           "190", "191", "192", "193", "194", "200", "201", "202", "203", "204", "205", 
                           "206", "207", "208"], ["1950", "1951", "1952", "1953", "1954", "1955", "1956", "1957", "1958", "2386"])
        self.CCI_LD_9 = ([], ["4560", "4561", "4562", "5722", "5723", "5724", "5725", "5726", "5727", "5728"])
        self.CCI_SOLID_TUMOR_9 = (["196", "197", "198", "199"], [])
        self.CCI_HIV_9 = (["042", "043", "044"], [])
        self.CCI_LIST_9 = (self.CCI_MI_9, self.CCI_CHF_9, self.CCI_PVD_9, self.CCI_CD_9, self.CCI_DEM_9, self.CCI_CPD_9, self.CCI_RHEUM_9, 
                           self.CCI_PUD_9, self.CCI_MLD_9, self.CCI_DIAB_NO_COMP_9, self.CCI_DIAB_W_COMP_9, self.CCI_PLEG_9, 
                           self.CCI_RENAL_9, self.CCI_MALIG_9, self.CCI_LD_9, self.CCI_SOLID_TUMOR_9, self.CCI_HIV_9)
        self.CCI_CLASS1 = ["CCI_MI","CCI_CHF","CCI_PVD","CCI_CD","CCI_DEM","CCI_CPD","CCI_RHEUM","CCI_PUD","CCI_MLD","CCI_DIAB_NO_COMP"]
        self.CCI_CLASS2 = ["CCI_PLEG","CCI_DIAB_W_COMP","CCI_RENAL","CCI_MALIG"]
        self.CCI_CLASS3 = ["CCI_LD"]
        self.CCI_CLASS6 = ["CCI_SOLID_TUMOR","CCI_HIV"]





