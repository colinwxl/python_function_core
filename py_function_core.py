# -*- coding: utf-8 -*-
"""
py_function_core.py
"""
import linecache
from itertools import islice
import os
import gzip
import bz2
import zipfile
import tarfile
import requests
#  import pandas as pd
#  import codecs

def toFloat(x,logical_exp):
    if logical_exp:
        return float(x)
    else:
        return x

def getSep(fname, default=True):
    if fname.endswith('.txt'):
        return '\t'
    elif fname.endswith('.csv'):
        return ','
    else:
        if default:
            return '\t'
        else:
            print('[Error] Wrong file type!.\nA .txt format or .csv format file is needed!\n')
            raise TypeError

def getHeader(fname):
    """Return list of the 1st line in a file"""
    return linecache.getline(fname, 1).strip().split(getSep(fname))

def list2dict(lst, n=0, ncol=0, isnum = False):
    odict = {}
    if n == 0:
        odict[lst[ncol]] = lst[ncol+1:]
    if n > 0:
        odict[tuple(lst[:n+1])] = lst[n+1:]
    return odict

def file_nth_col2list(fname,n=0):
    with open(fname,'r') as f:
        return [line.strip().split(getSep(fname))[n] for line in f]

def lst2file(lst,fname):
    with open(fname,'w') as f:
        for i in lst:
            f.write(str(i)+'\n')

def is_list(items):
        return type(items) is list
    
def file2dict(fname, header=True, keys='single', ncol=0, isnum = True):
    """Changes a Table(txt, csv) to a dictionary, 
    use the first fields in lines to be the parameter keys if keys = 'single', 
    or (1st filed, 2nd field) to be the keys parameter if keys = 'pair', 
    a list contains the left fields as the values
    """
    fdict = {}
    sep = getSep(fname)
    with open(fname, 'r') as f:
        start = 1 if header else 0
        for line in islice(f, start, None):
            if keys == 'single':
                fdict.update(list2dict(line.strip().split(sep),n=0,ncol=ncol))
            if keys == 'pair':
                fdict.update(list2dict(line.strip().split(sep),n=1))
    return fdict

class CompressTools:
    def __init__(self, bufSize):
        self.bufSize = bufSize
        self.fin = None
        self.fout = None
        self.ipath = None
        self.opath = None
    
    def __in2out(self):
        while True:
            buf = self.fin.read(self.bufSize)
            if len(buf) < 1:
                break
            self.fout.write(buf)
        self.fin.close()
        self.fout.close()

    def gzip_compress(self, ifile, ofile=None):
        if not ofile: ofile = ifile + '.gz'
        self.fin = open(ifile, 'rb')
        self.fout = gzip.open(ofile, 'wb')
        self.__in2out()

    def gzip_decompress(self, ifile, ofile=None):
        if not ofile: ofile = os.path.splitext(ifile)[0]
        self.fin = gzip.open(ifile, 'rb')
        self.fout = open(ofile, 'wb')
        self.__in2out()

    def bzip2_compress(self, ifile, ofile=None):
        if not ofile: ofile = ifile + '.bz2'
        self.fin = open(ifile, 'rb')
        self.fout = bz2.open(ofile, 'wb')
        self.__in2out()

    def bzip2_decompress(self, ifile, ofile=None):
        if not ofile: ofile = os.path.splitext(ifile)[0]
        self.fin = bz2.open(ifile, 'rb')
        self.fout = open(ofile, 'wb')
        self.__in2out()

    def zip_compress(self, ipath, opath=None):
        if not opath: opath = ipath + '.zip'
        file_lst = os.listdir(ipath)
        zip_file_object = zipfile.ZipFile(opath, 'w')
        for f in file_lst:
            zip_file_object.write(f)
        zip_file_object.close()

    def zip_decompress(self, ipath, opath=None):
        if not opath: opath = os.path.splitext(ipath)[0]
        zip_files = zipfile.ZipFile(ipath, 'r')
        zip_files.extractall(opath)
        zip_files.close()
        
    def multi_compress(self, ipath, opath, suffix='.gz', depth=1):
        n = 0
        if suffix == '.zip':
            ipath_iter = os.walk(ipath)
            while n < depth:
                n += 1
                (dirpath, subdirs, files) = next(ipath_iter)
                for d in subdirs:
                    file_path = os.path.join(dirpath, d)
                    new_file_path = os.path.join(opath, d + suffix)
                    self.zip_compress(file_path, new_file_path)
        else:
            for (dirpath, subdirs, files) in os.walk(ipath):
                for filename in files:
                    file_path = os.path.join(dirpath, filename)
                    new_file_path = os.path.join(opath, filename +suffix)
                    if suffix == ".gz":
                        self.gzip_compress(file_path, new_file_path)
                    elif suffix == ".bz2":
                        self.bzip2_compress(file_path, new_file_path)

    def multi_decompress(self, ipath, opath):
        for (dirpath, subdirs, files) in os.walk(ipath):
            for filename in files:
                file_path = os.path.join(dirpath, filename)
                _path, suffix = os.path.splitext(dirpath + filename)
                new_file_path = os.path.join(opath,
                        os.path.splitext(filename)[0])
                if suffix == '.zip':
                    self.zip_decompress(file_path, new_file_path)
                elif suffix == '.gz':
                    self.gzip_decompress(file_path, new_file_path)
                elif suffix == '.bz2':
                    self.bzip2_decompress(file_path, new_file_path)
    
    def targz_compress(self, ofile, ipath):
        with tarfile.open(ofile, 'w:gz') as tar:
            tar.add(ipath, arcname=os.path.basename(ipath))

    def tar_decompress(self, ifile, opath='.'):
        with tarfile.open(ifile) as tar:
            def is_within_directory(directory, target):
                
                abs_directory = os.path.abspath(directory)
                abs_target = os.path.abspath(target)
            
                prefix = os.path.commonprefix([abs_directory, abs_target])
                
                return prefix == abs_directory
            
            def safe_extract(tar, path=".", members=None, *, numeric_owner=False):
            
                for member in tar.getmembers():
                    member_path = os.path.join(path, member.name)
                    if not is_within_directory(path, member_path):
                        raise Exception("Attempted Path Traversal in Tar File")
            
                tar.extractall(path, members, numeric_owner=numeric_owner) 
                
            
            safe_extract(tar, path=opath)

    def tarbz2_compress(self, ofile, ipath):
        with tarfile.open(ofile, 'w:bz2') as tar:
            tar.add(ipath, arcname=os.path.basename(ipath))
