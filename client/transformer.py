import numpy as np
from numpy import add_docstring
import pandas as pd 

df = pd.read_csv("./client/application.csv",encoding='unicode_escape')

df['json'] = df.to_json(orient='records', lines=True).splitlines()

dfjson = df['json']


np.savetxt(r'./client/application.txt',dfjson.values, fmt = '%s')