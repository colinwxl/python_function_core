import os
import gzip
import bz2
import zipfile
import tarfile

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
            tar.extractall(path=opath)

    def tarbz2_compress(self, ofile, ipath):
        with tarfile.open(ofile, 'w:bz2') as tar:
            tar.add(ipath, arcname=os.path.basename(ipath))
