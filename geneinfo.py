# -*- coding: utf-8 -*-
"""
Created on Tue Sep 26 12:13:32 2017

@author: colinwxl
"""
import gzip
from itertools import islice
import re
#import os
#os.chdir("D:/work/Scripts")

taxid_dict = {"hsa":9606}

class GeneInfo:
    def __init__(self, org="hsa"):
        self.org = org
        self.taxid = taxid_dict[self.org]
        self.geneinfo_f = "./gene_info.gz"
        self.gene_uniprot = "./uniprot2geneid.gz"
        self.geneinfo_l = []
        self._parse()
        
    def _parse(self):
        with gzip.open(self.geneinfo_f, 'rt') as f:
            for line in islice(f, 1, None):
                taxid, geneid, symbol, o1, synonyms, dbxrefs, o2, o3, o4, o5, o6, fullname, others = line.strip().split('\t', 12)
                synonyms = '|'.join([symbol, synonyms])
                ensembl = ""
                match = re.match(r'.*\|Ensembl:(ENSG\d{11})\|?.*', dbxrefs)
                if match:
                    ensembl = match.group(1)
                self.geneinfo_l.append({'entrez': geneid,
                                        'symbol': symbol,
                                        'synonyms': synonyms,
                                        'ensembl': ensembl,
                                        'fullname': fullname})
    
    def idsconvert(self, id_lst, iType, tType):
        outdict = {}
        if iType == "synonyms":
            for i in self.geneinfo_l:
                coset = set(i["synonyms"].split('|')) & set(id_lst)
                if coset:
                    if list(coset)[0] in outdict:
                        outdict[list(coset)[0]].append(i[tType])
                    else:
                        outdict[list(coset)[0]] = [i[tType]]
        else:
            for i in self.geneinfo_l:
                if i[iType] in id_lst:
                    outdict[i[iType]] = i[tType]
        return outdict
    
    def idconvert(self, item, iType, tType):
        outdict = {}
        if iType == "synonyms":
            for i in self.geneinfo_l:
                if item in i["synonyms"]:
                    if item in outdict:
                        outdict[item].append(i[tType])
                    else:
                        outdict[item] = [i[tType]]
        else:
            for i in self.geneinfo_l:
                if i[iType] == item:
                    outdict[item] = i[tType]
                    break                    
        return outdict
    
    def get_record(self, item, iType):
        outlst = []
        if iType == "synonyms":
            for i in self.geneinfo_l:
                if item in i["synonyms"].split('|'):
                    outlst.append(i)
        else:
            for i in self.geneinfo_l:
                if i[iType] == item:
                    return i
        return outlst
    
    def get_records(self, id_lst, iType):
        outlst = []
        if iType == "synonyms":
            for item in id_lst:
                records = self.get_record(item,iType)
                for record in records:
                    outlst.append(record)
        else:
            for item in id_lst:
                outlst.append(self.get_record(item,iType))
        return outlst
