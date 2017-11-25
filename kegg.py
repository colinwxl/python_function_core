# -*- coding: utf-8 -*-
"""
Created on Mon Sep 25 17:43:42 2017

@author: colinwxl
"""
import os
import gzip
import geneinfo
from itertools import islice
import py_function_core
import requests
from lxml import etree
#  os.chdir("D:/work/Scripts")
com = py_function_core.CompressTools(1024*100)
def file2dict(pairfile = ""):
    pdict = {}
    with gzip.open(pairfile,'rt') as f:
        for line in f:
            key = line.strip('\n').split('\t')[0]
            value = line.strip('\n').split('\t')[1]
            if key in pdict:
                pdict[key].append(value)
            else:
                pdict[key] = [value]
    return pdict

def is_list(items):
        return type(items) is list
    
def get_subdict(keys, indict):
    outdict = {}
    for key in keys:
        outdict[key] = indict.get(key,[])
    return outdict

#class UPDATE:
#    def __init__(self):
#        pass    

class KEGG:
    def __init__(self, update=False):
        self.g2k_file = "/home/wuxiaolong/work/DATA/kegg/hsa/pathway/gene2pathway.gz"
        self.k2g_file = "/home/wuxiaolong/work/DATA/kegg/hsa/pathway/pathway2gene.gz"
        self.plst_file = "/home/wuxiaolong/work/DATA/kegg/hsa/pathway/pathway2gene"
        self.knetwork_file = "/home/wuxiaolong/work/DATA/kegg/hsa/pathway/wwi_func.txt"
        if not os.path.exists(self.g2k_file):
            self.get_kegg_api('http://rest.kegg.jp/link/pathway/hsa', self.g2k_file.split('.')[0])
        if not os.path.exists(self.k2g_file):
            self.get_kegg_api('http://rest.kegg.jp/link/hsa/pathway', self.k2g_file.split('.')[0])
        self.kgml_path = '/home/wuxiaolong/work/DATA/kegg/hsa/kgml'
        if not os.path.exists(self.kgml_path):
            os.mkdir(self.kgml_path)
        self.g2k_dict = {}
        self.k2g_dict = {}
        self.bg_k2g_dict = {}
        self.bg_g2K_dict = {}
        self.tmp_dict = {}
    
    def get_item(self, items, transType="GeneID2Kegg"):
        if transType == "GeneID2Kegg":
            self.g2k_dict = file2dict(self.g2k_file)
            if not is_list(items): return self.g2k_dict.get(items,[])
            self.tmp_dict = get_subdict(items, self.g2k_dict)
            return self.tmp_dict     
        if transType == "Kegg2GeneID":
            self.k2g_dict = file2dict(self.k2g_file)
            if not is_list(items): return self.k2g_dict.get(items,[])
            self.tmp_dict = get_subdict(items, self.k2g_dict)
            return self.tmp_dict
           
    def get_all_result(self, idict=dict()):
        if not idict:
            idict = self.tmp_dict
        outdict = set()
        for key in idict:
            outdict = outdict | set(idict[key])
        return list(outdict)
    
    def symbol2geneid(self, id_lst):
        g_info = geneinfo.GeneInfo()
        g_dict = g_info.idsconvert(id_lst, iType="symbol", tType="entrez")
        return [g_dict[i] for i in id_lst]
    
    def geneid2symbol(self, id_lst):
        g_info = geneinfo.GeneInfo()
        g_dict = g_info.idsconvert(id_lst, iType="entrez", tType="symbol")
        return [g_dict[i] for i in id_lst]
    
    def statistic_info(self):
        if not self.g2k_dict:
            self.g2k_dict = file2dict(self.g2k_file)
        if not self.k2g_dict:
            self.k2g_dict = file2dict(self.k2g_file)
        print("Pathway_Number: " + str(len(self.k2g_dict)) + '\n'+
              "Gene_Number: " + str(len(self.g2k_dict)) + '\n')

    def get_all_pathway_list(self):
        if not self.k2g_dict:
            self.k2g_dict = file2dict(self.k2g_file)
        return list(self.k2g_dict.keys())

    def get_all_gene_list(self):
        if not self.g2k_dict:
            self.g2k_dict = file2dict(self.g2k_file)
        return list(self.g2k_dict.keys())
    
    def kegg_enrichment():
        pass

    def get_kegg_api(self, url, ofile, compress=True):
        html = requests.get(url)
        k_text = html.text
        k_text = k_text.replace('hsa:','')
        k_text = k_text.replace('path:','')
        with open(ofile, 'w') as f:
            f.write(k_text)
        if compress:
            com.gzip_compress(ofile)

    def download_KGML(self, opath=None, species='hsa'): # quite slow
        if not opath:
            opath = self.kgml_path
        if not os.path.exists(self.plst_file):
            self.get_kegg_api('http://rest.kegg.jp/list/pathway/'+species, self.plst_file)
        plst = ['path:'+i for i in py_function_core.file_nth_col2list(self.plst_file)]
        for p in plst:
            self.get_kegg_api('http://rest.kegg.jp/get/'+ p +'/kgml', os.path.join(opath, p.split(':')[-1]+'.xml'), compress=False)

    def get_pathway_func_network(self, ofile=None):
        if not ofile:
            ofile = self.knetwork_file
        if not os.listdir(self.kgml_path):
            self.download_KGML()
        edges_list = []
        for f in os.listdir(self.kgml_path):
            xml = etree.parse(os.path.join(self.kgml_path,f))
            for node in xml.xpath('//entry[@type="map"]'):
                p1 = f.split('.')[0]
                p2 = node.get('name').split(':')[-1]
                if p2.find('map') != -1: continue
                if p1 == p2: continue
                if ((p1,p2) in edges_list) or ((p2,p1) in edges_list): continue
                edges_list.append((p1,p2))
        with open(ofile,'w') as fout:
            for pa,pb in edges_list:
                fout.write('\t'.join([pa,pb])+'\n')

