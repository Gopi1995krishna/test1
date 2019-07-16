import requests
import argparse
import json
import urllib3
from functions import get_mcs_token, get_amce_token
from time import sleep

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning) 

STATUS_CHECKS_INTERVAL = 15
VERBOSE = False
DRYRUN = False
SKIP_CONFLICTING = False
DISMISS_COLLECTION_OBJECTS = False

####
INTERNAL_DEV_AUTH = None


def get_internal_auth_headers(instance):
    if INTERNAL_DEV_AUTH:
        return {"Authorization": INTERNAL_DEV_AUTH}
    internal_token = get_mcs_token(instance)
    return {"Authorization": "Bearer {}".format(internal_token)}


def log(obj):
    if VERBOSE: print(obj)


def prettyprint(obj):
    print(json.dumps(obj, indent=2, sort_keys=True))


def invoke_mcs(resource, call, headers = None):
    if headers is None:
        headers = {}
    src_instance = config.get("source_instance")
    url = "{}{}".format(src_instance.get("instance_url"), resource)
    headers.update(get_internal_auth_headers(src_instance))
    headers.update(config.get("storage_account"))
    log("\nConnecting to {}".format(url))
    log("Headers: {}".format(headers))
    response = call(url, headers)
    log("{}".format(response.request.body))
    log(response.status_code)
    log(response.text)
    return response.json()


def invoke_amce(resource, call, headers = None):
    if headers is None:
        headers = {}
    tgt_instance = config.get("target_instance")
    target_token = get_amce_token(tgt_instance)
    url = "{}{}".format(tgt_instance.get("instance_url"), resource)
    headers.update({"Authorization": "Bearer {}".format(target_token)})
    headers.update(config.get("storage_account"))
    log("\nConnecting to {}".format(url))
    log("Headers: {}".format(headers))
    response = call(url, headers)
    log("{}".format(response.request.body))
    log(response.status_code)
    if response.status_code != 204:
        log(response.text)
        return response.json()

def invoke_object_storage(resource, call, headers = None):
    if headers is None:
        headers = {}
    headers["Accept"] = "application/json"
    storage_account = config.get("storage_account")
    #{{storageBaseUrl}}/v1/{{storageAccount}}/Migration/{{storage_object}}
    url = "{}/v1/{}/{}".format(storage_account.get("X-Storage-BaseUrl"), storage_account.get("X-Storage-Account"), resource)
    # -H 'Authorization: Basic YXZpLmJvcnRoYWt1ckBvcmFjbGUuY29tOkRIXURVTEU1Vi5wYmdpTEt2QTdf'
    user = storage_account.get("X-Storage-User")
    password = storage_account.get("X-Storage-Password")
    log("\nConnecting to {}".format(url))
    log("Headers:{}".format(headers))
    response = call(url, headers, user, password)
    log("{}".format(response.request.body))
    log(response.status_code)
    return response

####


def export():
    resource = "/mobile/tools/1.0/export/migration"
    call = lambda url, headers: requests.post(url, headers=headers, json=config.get("scope"))
    headers = config.get("security");
    return invoke_mcs(resource, call, headers)


def status_export(bundle):
    resource = "/mobile/tools/1.0/export/migration/{}".format(bundle)
    call = lambda url, headers: requests.get(url, headers=headers, verify=False)
    return invoke_mcs(resource, call)


def list():
    resource = "/mobile/tools/1.0/import/migration/list"
    call = lambda url, headers: requests.get(url, headers=headers, verify=False)
    return invoke_amce(resource, call)


def install(bundle):
    resource = "/mobile/tools/1.0/import/migration/install"
    json = {"name": bundle, "dryRun": DRYRUN}
    # add only if true to support AMC before 18.4.5
    if SKIP_CONFLICTING: 
        json["skipConflicting"] = SKIP_CONFLICTING
        json["collections"] = {}
        json["collections"]["deleteObjectsWithoutUser"] = DISMISS_COLLECTION_OBJECTS
    call = lambda url, headers: requests.post(url, headers=headers, json=json, verify=False)
    headers = config.get("security");
    return invoke_amce(resource, call, headers)


def status_install(bundle):
    resource = "/mobile/tools/1.0/import/migration/{}".format(bundle)
    call = lambda url, headers: requests.get(url, headers=headers, verify=False)
    return invoke_amce(resource, call)

# cleanup operations

def query_assets(filter = ""):
    resource = "/mobile/tools/1.0/assets?{}".format(filter)
    call = lambda url, headers: requests.get(url, headers=headers, verify=False)
    paged = invoke_amce(resource, call)
    if "items" in paged:
        return paged["items"]
    else:
        return []

def query_profiles(filter = ""):
    resource = "/mobile/tools/1.0/clients/profiles".format(filter)
    call = lambda url, headers: requests.get(url, headers=headers, verify=False)
    paged = invoke_amce(resource, call)
    if "items" in paged:
        return paged["items"]
    else:
        return []

def remove_profile(id):
    resource = "/mobile/tools/1.0/clients/profiles/{}".format(id)
    call = lambda url, headers: requests.delete(url, headers=headers, verify=False)
    return invoke_amce(resource, call)

def get_trash_dependencies(ids):
    resource = "/mobile/tools/1.0/assets/trashDependencies"
    call = lambda url, headers: requests.post(url, headers=headers, json={"ids": ids}, verify=False)
    return invoke_amce(resource, call)

def trash(dependencies):
    resource = "/mobile/tools/1.0/assets/trashing"
    call = lambda url, headers: requests.post(url, headers=headers, json={"actionComment": "cleanup", "dependencies":dependencies}, verify=False)
    return invoke_amce(resource, call)

def get_purge_dependencies(ids):
    resource = "/mobile/tools/1.0/assets/purgeDependencies"
    call = lambda url, headers: requests.post(url, headers=headers, json={"ids": ids}, verify=False)
    return invoke_amce(resource, call)

def purge(dependencies):
    resource = "/mobile/tools/1.0/assets/purging"
    call = lambda url, headers: requests.post(url, headers=headers, json={"actionComment": "cleanup", "dependencies":dependencies}, verify=False)
    return invoke_amce(resource, call)

def chunks(list, n):
    for i in range(0, len(list), n):
        yield list[i:i+n]

def trash_assets(assets):
    for assets_chunk in chunks(assets, 20):
        ids = [asset["id"] for asset in assets_chunk]
        # trash
        trash_dependencies = get_trash_dependencies(ids)
        trash(trash_dependencies)
        # purge
        purge_dependencies = get_purge_dependencies(ids)
        purge(purge_dependencies)


def clean_packages():
    assets = query_assets("assetTypes=Package&filterType=NOT_TRASH")
    trash_assets(assets)

def clean_clients():
    assets = query_assets("assetTypes=Client&filterType=NOT_TRASH")
    trash_assets(assets)

def clean_profile():
    profiles = query_profiles()
    for profile in profiles:
        remove_profile(profile["id"])

def clean_MBE():
    assets = query_assets("assetTypes=MobileBackend&filterType=NOT_TRASH")
    trash_assets(assets)

def clean_api():
    assets = query_assets("assetTypes=API&apiTypes=custom&filterType=NOT_TRASH")
    trash_assets(assets)

def clean_connector():
    assets = query_assets("assetTypes=Connector&filterType=NOT_TRASH")
    trash_assets(assets)

def clean_collection():
    assets = query_assets("assetTypes=StorageCollection&filterType=NOT_TRASH")
    trash_assets(assets)

def clean_implementation():
    assets = query_assets("assetTypes=APIImplementation&filterType=NOT_TRASH")
    trash_assets(assets)

def purge_trashed():
    assets = query_assets("filterType=TRASH")
    for assets_chunk in chunks(assets, 20):
        ids = [asset["id"] for asset in assets_chunk]
        # purge
        purge_dependencies = get_purge_dependencies(ids)
        purge(purge_dependencies)

def query_location_type(type):
    resource = "/mobile/tools/1.0/location/{}/query".format(type)
    call = lambda url, headers: requests.post(url, headers=headers, json={}, verify=False)
    paged = invoke_amce(resource, call)
    if "items" in paged:
        return paged["items"]
    else:
        return []

def remove_location_type(type, id):
    resource = "/mobile/tools/1.0/location/{}/{}".format(type, id)
    call = lambda url, headers: requests.delete(url, headers=headers, verify=False)
    return invoke_amce(resource, call)


def clean_location_type(type):
    assert type.lower() in ["places", "assets", "devices"], "Invalid location type"

    artifacts = query_location_type(type)
    while len(artifacts) > 0:
        for artifact in artifacts:
            remove_location_type(type, artifact["id"])
        artifacts = query_location_type(type)

def clean_location():
    clean_location_type("places")
    clean_location_type("assets")
    clean_location_type("devices")

def cleanup():
    clean_packages()
    clean_clients()
    clean_profile()
    clean_MBE()
    clean_api()
    clean_connector()
    clean_implementation()
    clean_collection()
    purge_trashed()
    clean_location()
    return "SUCCESS"

# storage operations
def cleanup_storage(object_path = None):
    objects = list_storage_objects(object_path)
    return delete_storage_objects(objects)

def list_storage_objects(object_path = None):
    objects = []
    storage_objects_json = read_storage_object_json(object_path)
    for object_json in storage_objects_json:
        objects.append(object_json["name"])
    return objects

def read_storage_object_json(object_path = None):
    storage_container = config.get("storage_account").get("X-Storage-Container")
    resource = "{}?format=json".format(storage_container) if object_path == None \
        else "{}?prefix={}&path={}&format=json".format(storage_container, object_path, object_path)
    call = lambda url, headers, user, password: requests.get(url, headers=headers, auth=(user, password), verify=False)
    response = invoke_object_storage(resource, call)
    log(response.text)
    return response.json()

def delete_storage_objects(objects = []):
    if (len(objects) == 0):
        print("Nothing to delete in Object Storage")
        return "SUCCESS"
    storage_container = config.get("storage_account").get("X-Storage-Container")
    # expected payload:
    # Migration/migration_2018121016351748/export_status.json
    # Migration/migration_2018121016351748/import_status.json
    payload_text = storage_container + "/" + "\n{}/".format(storage_container).join(objects)
    headers = {"Content-Type":"text/plain"}
    call = lambda url, headers, user, password: requests.delete(url, data=payload_text, headers=headers, auth=(user, password), verify=False)
    response = invoke_object_storage("?bulk-delete&format=json", call, headers)
    print("{}".format(response.text))
    json = response.json();
    # json can give status different from http response code
    status_message = json["Response Status"] if "Response Status" in json else "SUCCESS"
    return "SUCCESS" if status_message == "200 OK" else status_message

# The below section describes combined flows

def export_operation():
    export_details = export()
    bundle_name = export_details["name"]
    print("Exporting bundle {} from MCS. Its content is defined by the scope from config.json".format(bundle_name))
    status = "FIRST"
    while status != "SUCCESS" and status != "PART_FAILURE" and status != "WARNING" and status != "FAILURE" and status != "SKIP":
        sleep(STATUS_CHECKS_INTERVAL)
        status_details = status_export(bundle_name)
        status = status_details["status"]
        log("Status: {}".format(status))
    print("Export completed with status: {}".format(status))
    if status != "SUCCESS":
        print("Run \"migrate.py export_status {}\" to get status details.".format(bundle_name))
    return bundle_name, status


def install_operation(bundle):
    install_details = install(bundle)
    status = "FIRST"
    while status != "SUCCESS" and status != "PART_FAILURE" and status != "WARNING" and status != "FAILURE" and status != "SKIP":
        sleep(STATUS_CHECKS_INTERVAL)
        status_details = status_install(bundle)
        status = status_details["status"]
        log("Status: {}".format(status))
    if status == "WARNING":
        print("Installation successfully completed with status: {}. Please review detailed status report.".format(status))
    else:
        print("Installation completed with status: {}".format(status))
    if status != "SUCCESS":
        print("Run \"migrate.py install_status {}\" to get status details.".format(bundle))
    if "analytics" in status_details and status_details["analytics"]["status"] == "SUCCESS":
        print("WARNING: Analytics data is migrated. Repeated migration will affect space available in target instance.")
    # guide user to --skip-conflicting option if metadata migration status has IMPORTED_CLIENT_ID_PLATFORM_VERSION conflicts
    return status


def migrate_operation():
    bundle_name, export_status = export_operation()
    if export_status != "SUCCESS" and export_status != "WARNING":
        print("ERROR: Migrate from {} failed on export with status {}".format(config.get("source_instance"), export_status))
        exit(-1)
    status = install_operation(bundle_name)
    print("Migration completed with overall status: {}".format(status))

    return status


def parse_args(commands):
    parser = argparse.ArgumentParser()
    parser.add_argument("command",
                        choices=commands,
                        help="Specifies the command. See detailed help for supported values.")
    parser.add_argument("bundle_name", nargs='?',
                        help="bundle name, used only by a few commands, such as install or install_status")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Print detailed output")
    parser.add_argument("-n", "--dry-run", action="store_true",
                        help="Do not modify target instance. Used to detect conflicts before migration")
    parser.add_argument("-s", "--skip-conflicting", action="store_true",
                        help="Skip"
                             " (1) Clint metadata for MCS clients conflicting by combination of platform, client ID and version by dismissing both clients"
                             " (2) Owner user resolution for User-Isolated Collection objects if object owners' username not in IDCS. Add '-d' to dismiss objects with unresolved owner")
    parser.add_argument("-d", "--dismiss-collection-obj", action="store_true",
                        help="Requires '-s'. Changes -s behavior to dismiss User-Isolated Collection objects if object owners' username not in IDCS instead of keeping such objects but skipping user resolution")
    return parser.parse_args()


if __name__ == "__main__":
    with open("config.json") as config_file:
        config = json.load(config_file)
    operations = {"migrate": migrate_operation,
                  "list": list,
                  "export": export_operation,
                  "install": install_operation,
                  "install_status": status_install,
                  "export_status": status_export,
                  "cleanup": cleanup,
                  "cleanup_storage": cleanup_storage}
    args = parse_args(operations.keys())
    VERBOSE = args.verbose
    DRYRUN = args.dry_run
    SKIP_CONFLICTING = args.skip_conflicting
    DISMISS_COLLECTION_OBJECTS = args.dismiss_collection_obj
    if DISMISS_COLLECTION_OBJECTS and not(SKIP_CONFLICTING):
        print("Option '-d'(--dismiss-collection-obj) requires '-s'(--skip-conflicting) to be also provided")
        exit(-1)
    if args.bundle_name:
        res = operations[args.command](args.bundle_name)
    else:
        res = operations[args.command]()
    prettyprint(res)
