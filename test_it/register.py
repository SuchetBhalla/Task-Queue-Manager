import warnings
import requests
import json
import sys

warnings.filterwarnings("ignore")
url= 'https://127.0.0.1:443'
user   = 'ying879'
api_key= None
# PING: is the API live?
res= requests.get(url = f'{url}/ping', verify= False)
if res is None:
    print('The API is not reachable.')
    sys.exit(1)
else:
    if res.status_code != 200:
        print('The reponse\'s status was not "200". Exiting ..')
        sys.exit(1)
    else:
        # if the APi is live, then register the user
        res= requests.post(url= f'{url}/register', json= {'email' : user}, verify= False)
        if res is not None:
            if res.status_code == 200:
                print('SUCCESS: user registered')
                api_key= json.loads(res.text)["api_key"]
            else:
                print(res.status_code, res.text)
                sys.exit(1)
        else:
            print('The endpoint/APi is unreachable.')
            sys.exit(1)

# create the file 'trigger.lua' for use with the tool 'wrk'
if api_key is None:
    print('An error occured')
    sys.exit(1)
txt= ('{"email": '
 f'"{user}",'
 f'"api_key": "{api_key}", "input": [1, 1, 1, 1, 1]'
 '}')
with open('trigger.lua', 'w') as f:
    f.write('wrk.method = "POST"')
    f.write(f"\nwrk.body = '{txt}'\n")
    f.write('wrk.headers["Content-Type"] = "application/json"')
print('SUCCESS: file generated "trigger.lua"')
