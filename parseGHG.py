import os
import re
import yaml
import json
import zipfile
import datetime
import xmltodict
import configparser
import pandas as pd
from io import TextIOWrapper
from readSystemConfig import pareseConfig

## Written by June Skeeter 11/23/2025
# This script can parse a zipped .ghg file to all sub-components and return relevant data values


# Requires a ghg file output by a LICOR logger
# self.modes:
# 1 - Parse Metadata
# 2 - Read data and dump to a timestamped pandas dataframe
# saveTo: self.mode must == 2, save a GHG file to specified directory with timestamp in name following format output by card convert
# depth:
# base - only files in "root" of ghg, sufficient for most needs
# full - includes subfolders, which gives access to eddypro and config files where present

# Key elements:
# Metadata - dict of header information
# Data - numpy array or pandas timestamped dataframe depending on self.mode
# Timestamp - numpy array in POSIX format from logger time


class parseGHG():
    def __init__(self,log=False):
        self.log=log
        c = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(c,'config_files','defaultMetadata.yml'),'r') as f:
            self.Metadata = yaml.safe_load(f)
        
    def parse(self,file,mode=1,saveTo=None,depth='base',verbose=False):
        self.mode = mode
        fn = os.path.split(file)[1].rsplit('.',1)[0]
        self.Metadata['Timestamp'] = pd.to_datetime(datetime.datetime.strptime(
                re.search('([0-9]{4}\-[0-9]{2}\-[0-9]{2}T[0-9]{6})',
                    fn).group(0),'%Y-%m-%dT%H%M%S')).strftime('%Y-%m-%d %H:%M')
        
        with zipfile.ZipFile(file, 'r') as ghgZip:
            subFiles=ghgZip.namelist()
            self.Metadata['Contents'] = {}
            if verbose == True:
                print(f'Contents of {file}: \n\n'+'\n'.join(f for f in subFiles))

            # Get all possible contents of ghg file, for now only concerned with .data and .metadata, can expand to biomet and config/calibration files later
            for self.file in subFiles:
                self.name = self.file.replace(fn,'').lstrip('-')
                with ghgZip.open(self.file) as f:
                    if self.file.endswith('.data') or self.file.endswith('.status'):
                        self.readDATA(f)
                    elif self.file.endswith('.metadata') or (self.file.endswith('processing') and depth == 'full'):
                        self.ini2dict(TextIOWrapper(f, 'utf-8'))
                    elif self.file.endswith('.conf') and depth == 'full':
                        self.Metadata['Contents'][self.name] = pareseConfig(f.readline().decode('ascii'))
                    elif self.file.endswith('.json') and depth == 'full':
                        self.Metadata['Contents'][self.name] = json.load(TextIOWrapper(f, 'utf-8'))
                    elif self.file.endswith('.log') and depth == 'full':
                        self.Metadata['Contents'][self.name] = TextIOWrapper(f, 'utf-8').read()
                    elif self.file.endswith('.xml') and depth == 'full':
                        self.Metadata['Contents'][self.name] = xmltodict.parse(f)
                    elif self.file.endswith('.csv') and depth == 'full':
                        if 'full_output' in self.file:
                            self.readEP(f,header=[0,1],skiprows=[0])
                        elif 'biomet' in self.file:
                            self.readEP(f,header=[0,1])
                        else:
                            self.readEP(f,header=[0])
        if self.mode >1:
            self.Data = {}
            for l,t in {'.data':['Date','Time'],
                        'biomet.data':['DATE','TIME'],
                        '.status':['Seconds','Nanoseconds']}.items():
                if l in self.Metadata['Contents'].keys():
                    self.Data[l] = {}
                    df = self.Metadata['Contents'][l].pop('Data')
                    if t[0].lower()=='date':
                        df.index = pd.to_datetime(df[t[0]]+' '+df[t[1]],format='%Y-%m-%d %H:%M:%S:%f')
                    else:
                        df.index = pd.to_datetime(df[t[0]]+df[t[1]]*1e-9,unit='s')
                    self.Data[l] = df.copy()           
            
    def readDATA(self,f):
        i = 0
        d = {}
        while i == 0 or len(Line)==2:
            Line = f.readline().decode('ascii').rstrip().lstrip()
            Line = Line.split('\t')
            if len(Line)==2:
                d[Line[0].replace(':','')]=Line[1]
            else:
                Header = {f'col_{i}':c for i,c in enumerate(Line)}
                d['Header'] = Header
            i += 1
        if self.mode > 1:
            df = pd.read_csv(f,header=None,sep='\t')
            df.columns=Line
            d['Data'] = df.copy()
        if self.Metadata['LoggerModel'] is None:
            self.Metadata['LoggerModel'] = d['Model']
        if self.Metadata['Table'] is None:
            self.Metadata['Table'] = 'Flux_Data'
        if self.Metadata['Timezone'] is None:
            self.Metadata['Timezone'] = d['Timezone']
        self.Metadata['Contents'][self.name]  = d

    def ini2dict(self,f):
        cfg = configparser.ConfigParser()
        cfg.read_file(f)
        d = cfg._sections
        self.Metadata['Contents'][self.name]  = d
        if 'Station' in d.keys():
            self.Metadata['SerialNo'] = d['Station']['logger_id']
            self.Metadata['StationName'] = d['Station']['station_name']
        if 'Timing' in d.keys():
            self.Metadata['Frequency'] = d['Timing']['acquisition_frequency'] + ' Hz'

    def readEP(self,f,header=None,skiprows=None):
        df = pd.read_csv(f,header=header,skiprows=skiprows,encoding='utf-8')
        if len(header) < 2:
            df.index=['value']
            d = df.to_dict()
        elif len(header) == 2:
            d = {}
            for c in df.columns:
                d[str(c[0])]={'unit':str(c[1]),
                        'value':df[c].tolist()[0]}
        self.Metadata['Contents'][self.name]  = d
