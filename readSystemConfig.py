import json
import numpy as np

## Written by June Skeeter 11/23/2024
# This code parses a .conf file that comes bundled in some .ghg files
# It works, but is not optimized
# These files (are (a proprietary (mess of) nested (parentheses)) that are parsed to ) a dict
# They contain calibration info among other things that are not accessible in the .metadata file

def getD(text):
    p = 0
    mx = 0
    for c in text:
        if c == '(':
            p += 1
        elif c == ')':
            p -= 1
        mx = max(p,mx)
    t = 0
    mxIX = []
    for i,c in enumerate(text):
        if c == '(':
            p += 1
        elif c == ')':
            p -= 1
        if p == mx and t == 0:
            t = 1
            mxIX.append(i)
        elif t == 1 and p < mx:
            mxIX.append(i)
            t = 0
    IX = np.array(mxIX).reshape(-1,2)
    deffs = {}
    for i,ix in enumerate(IX):
        key = f" {mx}_._0{i}0{i}, "
        deffs[key] = text[ix[0]:ix[1]+1]
    for j in range(i,-1,-1):
        key = f" {mx}_._0{j}0{j}, "
        text = text.replace(deffs[key],key)
        d = deffs[key].split(' ',1)
        if len(d)>1:
            deffs[key] = {d[0].replace('(',''):d[1].rstrip(')')}
        else:
            deffs[key] = d
    return(text,deffs,mx)

def recurRep(nuBld,Bld,verbose=False):
    for key,value in nuBld.items():
        if type(value) is str and len(value.split(',  '))>1:
            nuVals = []
            for v in value.split(','):
                vf = float(v.replace('_','').replace(',','').lstrip().rstrip())
                nuVal = Bld[vf].copy()
                if type(nuVal) == dict:
                    nuVal = recurRep(nuVal,Bld=Bld)
                nuVals.append(nuVal)
            nuBld[key] = nuVals
        elif type(value) is str and '_._' in value:
            vf = float(value.replace('_','').replace(',','').lstrip().rstrip())
            nuVal = Bld[vf].copy()
            if type(nuVal) == dict:
                nuVal = recurRep(nuVal,Bld=Bld)
            nuBld[key] = nuVal
        else:
            if value == 'TRUE' or value == 'FALSE':
                value = bool(value)
            nuBld[key] = value
    return(nuBld)

def pareseConfig(text):
    allDeffs = {}
    MX,mx = 0,0
    while mx > 1 or MX < 2:
        text,deffs,mx = getD(text)
        MX = max(MX,mx)
        allDeffs = allDeffs | deffs
        text = text.rstrip(', ')
        keys = list(allDeffs.keys())
        
    keys.sort()
    nuKeys = []
    Bld = {}
    for key in keys:
        nuKey = float(key.replace('_','').replace(',','').lstrip().rstrip())
        try:
            t = ('{"'+list(allDeffs[key].keys())[0]+'":['+list(allDeffs[key].values())[0].rstrip(', ')+']}')
            dv = json.loads(t)
        except:
            t = ('{"'+list(allDeffs[key].keys())[0]+'":"'+list(allDeffs[key].values())[0].rstrip(', ')+'"}')
            dv = json.loads(t)
        if type(list(dv.values())[0]) == list:
            if len(list(dv.values())[0])==0:
                dv[list(dv.keys())[0]]=""
            else:
                dv[list(dv.keys())[0]]=list(dv.values())[0][0]
        Bld[nuKey] = dv
        nuKeys.append(nuKey)
    nuKeys.sort(reverse=True)
    nuKeys = np.array(nuKeys)
    
    nuBld = recurRep(Bld[1.0].copy(),Bld)

    return(nuBld)
