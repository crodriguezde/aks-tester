# Import the needed credential and management objects from the libraries.
import argparse
import logging
import os
from pickle import FALSE
import subprocess
import sys
import tempfile
import time
import uuid
import backoff
import datetime
from pydoc import cli
from xmlrpc.client import boolean
from logging import FileHandler
from logging import Formatter

from azure.cli.core import get_default_cli
from azure.identity import AzureCliCredential
from azure.mgmt.authorization import AuthorizationManagementClient
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.containerservice import ContainerServiceClient
from azure.mgmt.msi import ManagedServiceIdentityClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.core.exceptions import ResourceNotFoundError
from azure.core.exceptions import HttpResponseError

class Decorator:
    def __init__(self):
        pass
    
    def __call__(self, fn):
        def timer(*args, **kwargs):
            start = time.time()
            result = fn(*args, **kwargs)
            end = time.time()
            logging.info(f'[ {fn.__name__:<24} {(end - start):>.4f}s ] {result}')
            #print(f'{fn.__name__} {(end - start):.4f}s')
            return result
        return timer

def azcli(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out,err = process.communicate()
    print(out)
    exit_code = process.returncode
    if exit_code and exit_code != 0:
        print(err)
        sys.exit(exit_code)
    else:
        return out

def az_cli (args_str):
    temp = tempfile.TemporaryFile()
    args = args_str.split()
    code = get_default_cli().invoke(args, None, temp)
    temp.seek(0)
    data = temp.read().strip()
    temp.close()
    return [code, data]


@Decorator()
def msi_get(cli_args):
    resource_client = ManagedServiceIdentityClient(cli_args.credential, cli_args.subscription_id)
    return resource_client.user_assigned_identities.get(
        cli_args.resource_group,
        cli_args.msi_name,
    )


@Decorator()
def msi_create(cli_args):
    resource_client = ManagedServiceIdentityClient(cli_args.credential, cli_args.subscription_id)
    return resource_client.user_assigned_identities.create_or_update(
        cli_args.resource_group,
        cli_args.msi_name,
        {
            "location": cli_args.region,
        }
    )


@Decorator()
def aks_get(cli_args):
    resource_client = ContainerServiceClient(cli_args.credential, cli_args.subscription_id)
    try:
        result = resource_client.container_services.get(
                    cli_args.resource_group,
                    cli_args.aks_name,
                )
        return True
    except:
        return False

@Decorator()
def aks_delete(cli_args):
    resource_client = ContainerServiceClient(cli_args.credential, cli_args.subscription_id)
    delete_async = resource_client.container_services.begin_delete(
                    cli_args.resource_group,
                    cli_args.aks_name,
                )
    delete_async.wait()
    return delete_async.result()


@Decorator()
def aks_create(cli_args, hostgrp, msi_principal_id):
    # Hack, no sdk for ADH yet.
    cmd = ['az', 'aks', 'create','-g',f'{cli_args.resource_group}','-n',f'{cli_args.aks_name}',
                      '--location', f'{cli_args.region}', '--kubernetes-version', '1.23.3',
                      '--nodepool-name', 'agentpool1', '--node-count', f'{cli_args.num_nodes}',
                      '--host-group-id' ,f'{hostgrp.id}', '--node-vm-size', f'{cli_args.node_sku}',
                      '--enable-managed-identity', '--assign-identity', f'{msi_principal_id}',
                      '--zones', '1']
    print(' '.join(cmd))
    response = azcli(cmd)
    return response

@Decorator()
def create_host_group(cli_args):
    resource_client = ComputeManagementClient(cli_args.credential, cli_args.subscription_id)
    result = resource_client.dedicated_host_groups.create_or_update(
        cli_args.resource_group,
        cli_args.host_group,
        {
            'location': cli_args.region, 
            'zones': [
                cli_args.zone
            ],
            'platform_fault_domain_count': 5,
            'support_automatic_placement': True,
        }
    )
    return result

@Decorator()
def host_group_get(cli_args):
    resource_client = ComputeManagementClient(cli_args.credential, cli_args.subscription_id)
    return resource_client.dedicated_host_groups.get(
        cli_args.resource_group,
        cli_args.host_group,
    )


@Decorator()
def create_host_in_group(cli_args, idx):
    # TODO: ResourceExistsError 
    resource_client = ComputeManagementClient(cli_args.credential, cli_args.subscription_id)
    create_async = resource_client.dedicated_hosts.begin_create_or_update(
        cli_args.resource_group,
        cli_args.host_group,
        f'host{idx}-auto', 
        {
            'platform_fault_domain': int(idx),
            'sku': {
                'name': 'DSv3-Type3'
            },
            'location': cli_args.region,
        },
    )
    create_async.wait()
    return create_async.result()

@Decorator()
def resource_group_exists(cli_args):
    # Obtain the management object for resources.
    resource_client = ResourceManagementClient(cli_args.credential, cli_args.subscription_id)

    # Provision the resource group.
    return resource_client.resource_groups.check_existence(
        cli_args.resource_group
    )



@Decorator()
def delete_resource_group(cli_args):
    # Obtain the management object for resources.
    resource_client = ResourceManagementClient(cli_args.credential, cli_args.subscription_id)
    # Provision the resource group.
    delete_async = resource_client.resource_groups.begin_delete(cli_args.resource_group)
    delete_async.wait()
    return delete_async.result()

@Decorator()
def resource_group_get(cli_args):
    # Retrieve subscription ID from environment variable.
    subscription_id = os.environ['AZURE_SUBSCRIPTION_ID']

    # Obtain the management object for resources.
    resource_client = ResourceManagementClient(cli_args.credential, cli_args.subscription_id)

    # Provision the resource group.
    return resource_client.resource_groups.get(
       cli_args.resource_group,
    )


@Decorator()
def create_resource_group(cli_args):
    # Retrieve subscription ID from environment variable.
    subscription_id = os.environ['AZURE_SUBSCRIPTION_ID']

    # Obtain the management object for resources.
    resource_client = ResourceManagementClient(cli_args.credential, cli_args.subscription_id)

    # Provision the resource group.
    return resource_client.resource_groups.create_or_update(
       cli_args.resource_group,
        {
            'location': cli_args.region
        }
    )


@Decorator()
def check_rg_role_assingment(cli_args, msi_principal_id):
    resource_client = AuthorizationManagementClient(cli_args.credential, cli_args.subscription_id)
    result = resource_client.role_assignments.list_for_resource_group(
        cli_args.resource_group,
    )
    for l in result:
       if l.properties.principal_id == msi_principal_id:
           return True 

    return False

@backoff.on_exception(backoff.expo, HttpResponseError, max_time=300)
@Decorator()
def msi_assign_role_to_rg(cli_args, resource_group,  msi_principal_id):
    resource_client = AuthorizationManagementClient(cli_args.credential, cli_args.subscription_id, '2018-01-01-preview')
    # Get "Contributor" built-in role as a RoleDefinition object
    role_name = 'Contributor'
    roles = list(resource_client.role_definitions.list(
        resource_group.id,
        filter="roleName eq '{}'".format(role_name)
    ))
    assert len(roles) == 1
    contributor_role = roles[0]

    return resource_client.role_assignments.create(
        resource_group.id,
        uuid.uuid4(), # Role assignment random name
        {
                'role_definition_id': contributor_role.id,
                'principal_id': msi_principal_id,
                'principal_type': "ServicePrincipal", 
        }
    )


@Decorator()
def storage_account_check(cli_args):
    resource_client = StorageManagementClient(cli_args.credential, cli_args.subscription_id)
    return resource_client.storage_accounts.get_properties(
        cli_args.resource_group,
        f'{cli_args.resource_group}store'
    )
\

@Decorator()
def storage_account_create(cli_args):
    resource_client = StorageManagementClient(cli_args.credential, cli_args.subscription_id)
    create_async = resource_client.storage_accounts.begin_create(
       cli_args.resource_group,
       cli_args.storage_name,
        {
            "location": cli_args.region,
            "kind": "StorageV2",
            "sku": {
                "name": "Standard_LRS"
            }
        }
    )
    create_async.wait()

    return create_async.result()

@Decorator()
def container_check(cli_args, store):
    resource_client = StorageManagementClient(cli_args.credential, cli_args.subscription_id)
    return resource_client.blob_containers.get(
        cli_args.resource_group,
        cli_args.storage_name,
        cli_args.container_name,
    )


@Decorator()
def container_create(cli_args, store):
    resource_client = StorageManagementClient(cli_args.credential, cli_args.subscription_id)
    return resource_client.blob_containers.create(
        cli_args.resource_group,
        cli_args.storage_name,
        cli_args.container_name,
        {},
    )


def main():
    application_insights = os.environ.get('APPLICATIONINSIGHTS_CONNECTION_STRING')
    if not application_insights != "":
        print("FOUND APPLICATIONS INSIGHTS")

    return

    file_formatter = logging.Formatter('[ {asctime:s} {levelname:>8s} ] {message}', style='{', datefmt="%Y-%m-%dT%H:%M:%S")
    logger = logging.getLogger()

    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    file_logger_file_handler = FileHandler(f'./logs/{time.strftime("%Y%m%d-%H%M%S")}-log.txt')
    file_logger_file_handler.setLevel(logging.DEBUG)
    file_logger_file_handler.setFormatter(file_formatter)
    logger.addHandler(file_logger_file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(file_formatter)
    stream_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)

    az_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    az_logger.setLevel(logging.WARNING)
    az_logger.addHandler(file_logger_file_handler)

    az_identity_logger = logging.getLogger("azure.identity._internal.decorators")
    az_identity_logger.setLevel(logging.WARNING)

    url_logger = logging.getLogger("urllib3.connectionpool")
    url_logger.setLevel(logging.WARNING)

    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--resource_group', help='resource group name', default='ScalabilityTest')
    parser.add_argument('-a', '--aks_name', help='aks cluster name', default='AKSScaleTestX')
    parser.add_argument('-j', '--host_group', help='name of the host group', default='HostGroup')
    parser.add_argument('-n', '--num_hosts', help='number of hosts in group', default=5, type=int)
    parser.add_argument('-m', '--num_nodes', help='number of nodes in aks', default=3, type=int)
    parser.add_argument('-i', '--subscription_id', help='subscription id', default='') 
    parser.add_argument('-c', '--create', help='create initial resources - resource-group, host group', action='store_true')
    parser.add_argument('--node_sku', help='VM sku name', default='Standard_D4s_v3')
    parser.add_argument('-r', '--region', help='region to deploy resources', default='southcentralus')
    parser.add_argument('-z', '--zone', help='zone to deploy resources [1,2,3]', default='1')
    parser.add_argument('-d', '--delete', help='delete resource group and exit', action='store_true')
    
    args = parser.parse_args()

    # Acquire a credential object using CLI-based authentication.
    args.credential = AzureCliCredential()
    
    if args.subscription_id == '':
        # Retrieve subscription ID from environment variable.
        args.subscription_id = os.environ['AZURE_SUBSCRIPTION_ID']\

    # All names will be prepended with the region
    args.resource_group += args.region.upper()
    args.aks_name += args.region.upper()
    args.host_group += args.region.upper()
    args.storage_name = 'scaletest79'
    args.container_name = 'logs'
    args.msi_name = uuid.uuid4()

    if args.delete:
        delete_resource_group(args) 
        return
    
    if args.create and resource_group_exists(args):
        delete_resource_group(args)
        
    if not resource_group_exists(args):
        create_resource_group(args)

    rg = resource_group_get(args)

    try:
        msi = msi_get(args)
    except:
        msi = msi_create(args)

    #try: 
    #    store = storage_account_check(args)
    #except:
    #    store = storage_account_create(args)

    #try:
    #    container = container_check(args, store)
    #except:
    #    container = container_create(args, store)

    try:
        hostgrp = host_group_get(args)
    except ResourceNotFoundError:
        # Hostgroup not found, create then
        hostgrp = create_host_group(args)
        for idx in range(args.num_hosts):
            create_host_in_group(args, str(idx))

    # HACK, after creating msi is not available, so we have to try to get it or wait for msi
    # to be created by azure
    # logging.debug("Cooling down for msi propagation 60s")
    # time.sleep(60)

    if not check_rg_role_assingment(args, msi.principal_id):
        msi_assign_role_to_rg(args, rg, msi.principal_id)
        

    if aks_get(args):
        aks_delete(args)
    aks_create(args, hostgrp, msi.id)

if __name__ == '__main__':
    main()
