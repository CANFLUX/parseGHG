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
from dataclasses import dataclass,field

try:
    from . import readSystemConfig
except:
    import readSystemConfig

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


def load():
    c = os.path.dirname(os.path.abspath(__file__))
    pth = os.path.join(c,'config_files','defaultMetadata.yml')
    with open(pth,'r') as f:
        defaults = yaml.safe_load(f)
    return(defaults)
@dataclass
class Metadata:
    log: bool = False
    verbose: bool = False
    mode: int = 1
    Metadata: dict = field(default_factory=load)
    Contents: dict = field(default_factory=lambda:{'data':None,
                                                'metadata':None,
                                                'biometdata':None,
                                                'biometmetadata':None,
                                                'system_config':{},
                                                'eddypro':{}})

class parseGHG(Metadata):
    def __init__(self,**kwds):
        super().__init__(**kwds)
        
    def parse(self,file,saveTo=None,depth='base',):
        try:
            with zipfile.ZipFile(file, 'r') as ghgZip:
                subFiles=ghgZip.namelist()
                if self.verbose == True:
                    print(f'Contents of {file}: \n\n'+'\n'.join(f for f in subFiles))
                
                fn = os.path.commonprefix([s for s in subFiles if len(os.path.split(s)[0])==0])  
                self.Metadata['Timestamp'] = pd.to_datetime(datetime.datetime.strptime(
                        re.search('([0-9]{4}\-[0-9]{2}\-[0-9]{2}T[0-9]{6})',
                            fn).group(0),'%Y-%m-%dT%H%M%S')).strftime('%Y-%m-%dT%H%M')
                # Get all possible contents of ghg file, for now only concerned with .data and .metadata, can expand to biomet and config/calibration files later
                for self.file in subFiles:
                    self.name = self.file.replace(fn,'').replace('.','').lstrip('-')
                    with ghgZip.open(self.file) as f:
                        if self.file.endswith('.data') or self.file.endswith('.status'):
                            self.readDATA(f)
                        elif self.file.endswith('.metadata') or (self.file.endswith('eddypro') and depth == 'full'):
                            self.ini2dict(TextIOWrapper(f, 'utf-8'))
                        elif self.file.endswith('.conf') and depth == 'full':
                            self.Contents[self.name] = readSystemConfig.pareseConfig(f.readline().decode('ascii'))
                        elif self.file.endswith('.json') and depth == 'full':
                            self.Contents[self.name] = json.load(TextIOWrapper(f, 'utf-8'))
                        elif self.file.endswith('.log') and depth == 'full':
                            self.Contents[self.name] = TextIOWrapper(f, 'utf-8').read()
                        elif self.file.endswith('.xml') and depth == 'full':
                            self.Contents[self.name] = xmltodict.parse(f)
                        elif self.file.endswith('.csv') and depth == 'full':
                            if 'full_output' in self.file:
                                self.readEP(f,header=[0,1],skiprows=[0])
                            elif 'biomet' in self.file:
                                self.readEP(f,header=[0,1])
                            else:
                                self.readEP(f,header=[0])
                        t = os.path.split(self.name)
                        if len(t[0])>0 and len(t[1])>0 and self.name in self.Contents.keys():
                            tmp = self.Contents.pop(self.name)
                            if t[0] not in self.Contents.keys():
                                self.Contents[t[0]] = {}
                            self.Contents[t[0]][t[1]] = tmp
        except:
            self.mode = 0
            print('unable to extract file')
        if self.mode >1:
            self.Data = {}
            for l,t in {'data':['Date','Time'],
                        'biometdata':['DATE','TIME'],
                        'li7700status':['SECONDS','NANOSECONDS']}.items():
                if l in self.Contents.keys():
                    self.Data[l] = {}
                    df = self.Contents[l].pop('Data')
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
                # Header = {f'col_{i}':c for i,c in enumerate(Line)}
                d['Header'] = Line
            i += 1
        if self.mode > 1:
            df = pd.read_csv(f,header=None,sep='\t')
            df.columns=Line
            d['Data'] = df.copy()
        if self.Metadata['Timezone'] is None:
            self.Metadata['Timezone'] = d['Timezone']
        if self.Metadata['Logger'] is None:
            self.Metadata['Logger'] = d['Model'].split(' ')[0]
        self.Contents[self.name]  = d

    def ini2dict(self,f):
        cfg = configparser.ConfigParser()
        cfg.read_file(f)
        d = cfg._sections
        self.Contents[self.name]  = d
        if 'Station' in d.keys():
            self.Metadata['SerialNo'] = d['Station']['logger_id']
            if d['Station']['station_name'] != '':
                self.Metadata['StationName'] = d['Station']['station_name']
            elif d['Site']['site_name'] != '':
                self.Metadata['StationName'] = d['Site']['site_name']
            else:
                self.Metadata['StationName'] = None
            self.Metadata['Program'] = 'Smartflux'+d['Station']['logger_sw_version']
        elif 'Site' in d.keys():
            self.Metadata['StationName'] = d['Site']['site_name']
        if 'Timing' in d.keys():
            self.Metadata['Frequency'] = str(1/float(d['Timing']['acquisition_frequency'])) + 's'

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
        self.Contents[self.name]  = d
