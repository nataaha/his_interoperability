import sys, argparse, os, json, csv, base64, requests, urllib.request
from collections import namedtuple
import datetime

class ConfigError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

def _json_object_hook(d):
    return namedtuple('X', d.keys())(*d.values())

def json2obj(data):
    return json.loads(data, object_hook=_json_object_hook)

def cmpT(t1, t2):
    return sorted(t1) == sorted(t2)
def compare_dhis2_sites(csvData=None,sites=None,operation='Add'):
    if csvData is not None and sites is not None:
        if operation.toLower() == 'add':
            pass
        elif operation.toLower() == 'edit' or operation.toLower() == 'update':
            pass
        else:
            pass
    else:
        pass
def validate_secrets(secrets):
    secrets_dhis=('username','password','baseurl')
    #return cmpT(secrets._fields, secrets_fields)
    #relax this to cater for secrets with or without service section
    return cmpT(secrets.dhis._fields, secrets_dhis)

def load_secrets(secrets_location):
    if os.path.isfile(secrets_location) and os.access(secrets_location, os.R_OK):
        json_data = open(secrets_location).read()
    else:
        raise ConfigError('Cannot load secrets')
        sys.exit('Aborting.')
    try:
        cleanUrl=json.loads(json_data)
        url = cleanUrl['dhis']['baseurl']
        url= url.rstrip('/')
        cleanUrl['dhis']['baseurl'] = url
        config = json2obj(json.dumps(cleanUrl))
    except ValueError as e:
        sys.exit('Secrets file looks to be mangeled.' + e)
    else:
        valid = validate_secrets(config)
        if valid:
            return config
        else:
            sys.exit('Secrets file is not valid.')

def read_csv_file(file_path):
    with open(file_path,encoding='utf-8') as f:
        #has_header = csv.Sniffer().has_header(f.read(1024))
        has_header = csv.Sniffer().has_header(f.readline())
        f.seek(0)
        reader = csv.reader(f,delimiter=",",quotechar='"')
        if has_header:
            next(reader)
        config = list(reader)
        for row in config:
            for i, y in enumerate(row):
                row[i] = y.strip()
    return config

# Get all sites (uid,name,code) for a country using country's Uid and store them in global variable
def get_country_sites(secrets,country_uid):
    all_sites = []
    try:
        get_request = requests.get(secrets.dhis.baseurl + "/api/organisationUnits.json?fields=:owner",auth=(secrets.dhis.username, secrets.dhis.password),params={"paging":"false","filter":"ancestors.id:eq:" + str(country_uid)})
        all_sites = get_request.json()
        return all_sites['organisationUnits']
    except urllib.request.URLError as e:
        print(e)
    return all_sites

# Get site with required details
def get_site(site_uid,all_sites):
    if len(all_sites) > 0:
        for site in all_sites:
            if site['id'] == site_uid:
                return site
        return None
    else:
        print("No country sites are available")
        return None

def validate_site_uid(secrets, site_uid,all_sites):
    if len(all_sites) > 0:
        for site in all_sites:
            if site['id'] == site_uid:
                return True
        return False
    else:
        print("No country sites are available")
        return False

def get_site_uid_from_code(code,all_sites):
    if len(all_sites) > 0:
        for site in all_sites:
            if site['code'] == code:
                return site['id']
        return None
    else:
        print("No country sites are available")
        return None

def check_site_existence(site_name, parent_uid,all_sites):
    # Clean up the name to remove special characters
    site_name = site_name.replace("'","''")
    if len(all_sites) > 0:
        for site in all_sites:
            if site['name'] == site_name and site['parent']['id'] == parent_uid:
                return site['id']
        return None
    else:
        print("No country sites are available")
        return None

def validate_add_site(secrets, new_site_uid, new_site_name, new_site_code, new_site_parent_uid, new_site_parent_code,all_sites):
    #validate minimum required fields for add
    if new_site_uid == "":
        print("Site UID Must be provided for add operation, please pre-generate uids for consistency")
        return False
    if new_site_name == "":
        print("Site Name Must be provided for add operation")
        return False
    if new_site_parent_uid == "" and new_site_parent_code == "":
        print("Parent Code or Parent UID must be provided for add operation")
        return False
    #get parent_uid if only parent_code was provided
    #This may be the case if the parent was created as part of the same payload
    if new_site_parent_uid == "":
        new_site_parent_uid = get_site_uid_from_code( new_site_parent_code,all_sites)
        #If we can't find the parent
        if new_site_parent_uid is None:
            print("Parent with code %s Not Found" %(new_site_parent_code))
            return False
    else:
        #parent_uid has been provided
        #check if it is valid
        if validate_site_uid(secrets, new_site_parent_uid,all_sites) is False:
            print("Parent with uid %s Not Found" %(new_site_parent_uid))
            return False
    #check if site already exists using provided name and parent
    site_uid = check_site_existence(new_site_name, new_site_parent_uid,all_sites)
    if site_uid is not None:
        print("org unit %s already exists with uid %s" %(new_site_name, site_uid))
        return False
    return True


def add_site_api(secrets, new_site_uid, new_site_name, new_site_shortname, new_site_code
                 , new_site_longitude, new_site_latitude
                 , new_site_parent_uid, new_site_parent_code,all_sites):
    strlog = ('%s:%s' % (secrets.dhis.username, secrets.dhis.password))
    base64string = base64.b64encode(bytes(strlog, 'ascii'))

    if validate_add_site(secrets,new_site_uid, new_site_name, new_site_code, new_site_parent_uid, new_site_parent_code,all_sites) == True:
        if new_site_parent_uid == "":
            new_site_parent_uid = get_site_uid_from_code( new_site_parent_code,all_sites)

        #initialize ou dictionary
        ou_dict = { 'id':'','parent':{'id':''}, 'name':'', 'shortName':'', 'openingDate':'1970-01-01', 'code':'', 'attributeValues':[]}
        if new_site_longitude != "" and new_site_latitude != "":
            coordinates = ("[%s,%s]" %(new_site_longitude,new_site_latitude))
            ou_dict['featureType'] = 'POINT'
            ou_dict['coordinates'] = coordinates
        ou_dict['parent']={'id': new_site_parent_uid }
        ou_dict['name'] = new_site_name
        ou_dict['id'] = new_site_uid
        if new_site_shortname != "":
            ou_dict['shortName'] = new_site_shortname
        else:
            ou_dict['shortName'] = new_site_name[:50]
            print("org unit %s shortname was not provided therefore was truncated to %s" %(new_site_name, new_site_name[:50]))
        if new_site_code != "":
            ou_dict['code'] = new_site_code

        else:
            pass

        return ou_dict

# Bulk add sites
def bulk_add_sites(secrets,sites):

    try:
        strlog = ('%s:%s' % (secrets.dhis.username, secrets.dhis.password))
        base64string = base64.b64encode(bytes(strlog, 'ascii'))
        post_request = requests.post(secrets.dhis.baseurl + "/api/metadata",data=json.dumps(sites),headers={"Content-Type": "application/json", "Authorization":"Basic %s" % base64string.decode('utf-8')})
        print(str(post_request.status_code))
        print(str(post_request.text))
    except urllib.request.URLError as e:
        print(e)
# Bulk update/relocate sites
def bulk_update_sites(secrets,sites,preheatMode):
    try:
        update_request = requests.post(secrets.dhis.baseurl + "/api/metadata?importStrategy=UPDATE&mergeMode=MERGE&preheatMode=" + preheatMode,data=json.dumps(sites),headers={"Content-Type": "application/json"},auth=(secrets.dhis.username, secrets.dhis.password))
        print(str(update_request.status_code))
        print(str(update_request.text))
    except urllib.request.URLError as e:
        print(e)

# get Key Value
def getKeyValue(site,key):
    if site.get(key) is not None:
        value = site[key]
    else:
        value = ""
    return value

def edit_site_sql(secrets
                  , current_site_uid, new_site_name, new_site_shortname
                  , new_site_code, new_site_parent_uid,new_site_longitude, new_site_latitude,all_sites):

    site = get_site(current_site_uid,all_sites)
    if validate_site_uid(secrets, current_site_uid,all_sites):
        #initialize ou dictionary for relocate, update, edit mohid, mohname
        ou_dict = site
        ou_dict['id'] = current_site_uid
        ou_dict['name'] = site['name']
        ou_dict['shortName'] = site['shortName']
        ou_dict['code']= getKeyValue(site,'code')
        ou_dict['parent']= site['parent']
        ou_dict['openingDate']= site['openingDate']
        ou_dict['featureType']=site['featureType']
        ou_dict['coordinates']= getKeyValue(site,'coordinates')
        ou_dict['lastUpdated'] = datetime.datetime.now().strftime('%Y-%m-%d')
        ou_dict['attributeValues'] = site['attributeValues']

        if new_site_name != "":
            new_site_name.replace("'","''")
            ou_dict['name'] = new_site_name
            if new_site_shortname == "":
                ou_dict['shortName'] = new_site_name[:50]
            else:
                new_site_shortname.replace("'","''")
                ou_dict['shortName'] = new_site_shortname[:50]

        if new_site_latitude != "" and new_site_longitude != "":
            coordinates = ("[%s, %s]" % (new_site_longitude, new_site_latitude))
            ou_dict['featureType'] = 'POINT'
            ou_dict['coordinates'] = coordinates
        # Relocate site if new parent uid is provided
        if new_site_parent_uid !="":
            #parent_uid has been provided
            #check if it is valid
            if validate_site_uid(secrets, new_site_parent_uid,all_sites) is False:
                print("Parent with uid %s Not Found" %(new_site_parent_uid))
                return False
            else:
                ou_dict['parent']= {'id': new_site_parent_uid }

        if new_site_code != "":
            #check if code is already in use. should be unique
            another_site_uid = get_site_uid_from_code( new_site_code,all_sites)
            if another_site_uid is not None:
                print("EDIT Failed: The site code specified for %s is already in use by %s." %(current_site_uid, another_site_uid))
                return
            else:
                ou_dict['code'] = new_site_code
        return ou_dict
    else:
        print('EDIT Failed: Org Unit with UID ' + current_site_uid + ' Does Not Exist')

def relocate_site_sql(secrets, current_site_uid, new_site_parent_uid,all_sites):
    #validate
    site = get_site(current_site_uid,all_sites)
    if validate_site_uid(secrets, current_site_uid,all_sites) and validate_site_uid(secrets, new_site_parent_uid,all_sites):
        ou_dict = site
        ou_dict['id'] = current_site_uid
        ou_dict['name'] = site['name']
        ou_dict['shortName'] = site['shortName']
        ou_dict['code']= getKeyValue(site,'code')
        ou_dict['openingDate']= site['openingDate']
        ou_dict['featureType']=site['featureType']
        ou_dict['coordinates']= getKeyValue(site,'coordinates')
        parent = {}
        parent['id'] = new_site_parent_uid
        ou_dict['parent']= parent
        ou_dict['created']= getKeyValue(site,'created')
        ou_dict['lastUpdated'] = datetime.datetime.now().strftime('%Y-%m-%d')
        ou_dict['attributeValues'] = site['attributeValues']
        return ou_dict
    else:
        print("Relocate Failed for Org Unit [%s]: The Parent OU [%s] Was Not Found!" %(current_site_uid, new_site_parent_uid))


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', "--secrets", help='Location of secrets file')
    parser.add_argument('-f', "--csv", help='Location of csv file with sites')
    parser.add_argument('-c', "--country_uid", help='UID of the operating unit')
    args = parser.parse_args()
    secrets = load_secrets(args.secrets)
    try:
        csv_file = read_csv_file(args.csv)

        country_uid = args.country_uid

        all_sites = get_country_sites(secrets,country_uid)

        # Add sites bulk object
        add_sites = { "organisationUnits" : []}
        # Edit sites bulk object
        edit_sites = { "organisationUnits" : []}
        # Edit sites bulk object
        relocate_sites = { "organisationUnits" : []}
        for row in csv_file:
            if row[0].lower() == 'add':
                print('Preparing Adding Org Unit ' + row[6] + ' uid ' + row[1])
                new_site_uid = row[1]
                new_site_name = row[2]
                new_site_shortname = row[7]
                new_site_code = row[8]
                new_site_longitude = row[6]
                new_site_latitude = row[5]
                new_site_parent_uid = row[3]
                new_site = add_site_api(secrets
                             , new_site_uid, new_site_name, new_site_shortname
                             , new_site_code, new_site_longitude, new_site_latitude
                             , new_site_parent_uid,all_sites)
                # Add site to array for bulk import
                if new_site:
                    add_sites['organisationUnits'].append(new_site)
            elif row[0].lower() == 'edit' or row[0].lower() == 'update' or row[0].lower() == 'rename':
                print('Editing Org Unit ' + row[1])
                current_site_uid = row[1]
                new_site_name = row[4]
                new_site_shortname = row[7]
                new_site_code = row[8]
                new_site_parent_uid = row[3]
                new_site_longitude = row[6]
                new_site_latitude = row[5]
                edit_site = edit_site_sql(secrets
                              , current_site_uid, new_site_name, new_site_shortname
                              , new_site_code,new_site_parent_uid,new_site_longitude, new_site_latitude,all_sites)
                # Add edit site to array for bulk import
                if edit_site:
                    edit_sites['organisationUnits'].append(edit_site)
            elif row[0].lower() == 'relocate':
                print('[%s] Relocating site [%s] to parent [%s]' %(datetime.datetime.now(), row[1],row[13]))
                current_site_uid = row[1]
                new_site_parent_uid = row[3]
                relocate_site = relocate_site_sql(secrets, current_site_uid, new_site_parent_uid,all_sites)
                # Add relocate site to array for bulk import
                if relocate_site:
                    relocate_sites['organisationUnits'].append(relocate_site)
            else:
                print('Could not execute directive for ' + str(row[0]))

        # Execute bulk adding sites - only if needed
        add_site_count = len(add_sites['organisationUnits'])
        if add_site_count > 0:
          print('Adding '+ str(add_site_count) + ' orgUnits')
          bulk_add_sites(secrets,add_sites)

        # Execute bulk editing sites - only if needed
        edit_site_count = len(edit_sites['organisationUnits'])
        if edit_site_count > 0:
          print("Clearing app cache before doing update to have directly updated attribute changes be reflected")
          get_request = requests.get(secrets.dhis.baseurl + "/api/maintenance/cacheClear",auth=(secrets.dhis.username, secrets.dhis.password))
          print('Editing '+ str(edit_site_count) + ' orgUnits')
          bulk_update_sites(secrets,edit_sites,"REFERENCE")

        # Execute bulk relocate of sites - only if needed
        relocate_site_count = len(relocate_sites['organisationUnits'])
        if relocate_site_count > 0:
          print('Relocating '+ str(relocate_site_count) + ' orgUnits')
          bulk_update_sites(secrets,relocate_sites,"REFERENCE")
        print("Updating OU Paths. It will take a while....")
        updatePaths = requests.put(secrets.dhis.baseurl + "/api/maintenance/ouPathsUpdate",auth=(secrets.dhis.username, secrets.dhis.password))
        if updatePaths.status_code == 204:
            print("Updated OU Paths.")
        else:
            print("Updating OU Paths failed. Login and manually run.")

    finally:
        pass
    success = True
    return success

if __name__ == "__main__":
    main(sys.argv[1:])
