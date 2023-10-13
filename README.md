# Python HIS Integration Engine
# Integration Layer for iHRIS, LMIS, DHIS2 Systems
# Use Case: eIDSR

# Import data
- completeness
 python3 ohahfr.py -f 'filename' -l 'secrets.json' -c 'reporting'
- Data generation (other countries)
 python3 ohahfr.py -f 'filename' -l 'secrets.json' -c 'hfr' -x 'n'
- Data generation (Zambia)
 python3 ohahfr.py -f 'filename' -l 'secrets.json' -c 'hfr' -x 'zm'

# Creating .secrets.json authentication file
 {

         "zmdhis":{
                 "username":"username",
                 "password":"password",
                 "url":"https://echo.jsi.com"
         }

 }
