import linecache
import json
import re

import requests


start = 1
end   = 5

i = start

while i <= end:

    line=linecache.getline("./application.txt", i)
    myjson = json.loads(line)
    print(myjson)
    response = requests.post("http://localhost:8000/applications", json=myjson)
    print(response.json())
    i+=1
