import os
import subprocess as sp
from shlex import quote as shq
import json
import urllib.parse
import yaml
import click


def url_join(url, path):
    parsed = urllib.parse.urlparse(url)
    return urllib.parse.urlunparse(parsed._replace(path=os.path.join(parsed.path, path)))


def run(args, **kwargs):
    assert 'shell' not in kwargs
    kwargs['shell'] = True
    assert 'check' not in kwargs
    kwargs['check'] = True
    print(f'running: {args}')
    sp.run(args, **kwargs)


def find_by_key(items, k, v):
    for item in items:
        if item[k] == v:
            return item
    raise ValueError(f'no item with {k}={v}')


def subst(command, k, v):
    return command.replace(k, v)


PROFILE = None
CONFIG = None


@click.group()
def main():
    global PROFILE, CONFIG

    with open('./profile.txt') as f:
        PROFILE = f.read().strip()
    print(f'PROFILE {PROFILE}')

    with open('./config.yaml') as f:
        CONFIG = yaml.safe_load(f.read())


def _create_vm(vm):
    name = vm['name']
    run(subst(vm['create'], '__NAME__', f'test-copy-{name}-{PROFILE}'))


@main.command()
@click.argument('name')
def create_vm(name):
    vm = find_by_key(CONFIG['vms'], 'name', name)
    _create_vm(vm)


@main.command()
def create_vms():
    for vm in CONFIG['vms']:
        _create_vm(vm)


def _delete_vm(vm):
    name = vm['name']
    run(subst(vm['delete'], '__NAME__', f'test-copy-{name}-{PROFILE}'))


@main.command()
@click.argument('name')
def delete_vm(name):
    vm = find_by_key(CONFIG['vms'], 'name', name)
    _delete_vm(vm)


@main.command()
def delete_vms():
    for vm in CONFIG['vms']:
        _delete_vm(vm)


def _run_on_vm(vm, cmd):
    cloud = vm['cloud']
    vm_name = f"test-copy-{vm['name']}-{PROFILE}"
    if cloud == 'gcp':
        run(f'gcloud compute ssh ubuntu@{vm_name} -- /bin/bash -c {shq(cmd)}')
    elif cloud == 'aws':
        id_file = CONFIG['aws-id-file']
        ip_cmd = f"aws ec2 describe-instances --region us-east-1 --filter Name=tag:Name,Values={vm_name} Name=instance-state-name,Values=pending,running | jq -r '.Reservations[0].Instances[0].PublicIpAddress'"
        run(f'ssh -o "StrictHostKeyChecking no" -i {id_file} ubuntu@$({ip_cmd}) -- /bin/bash -c {shq(shq(cmd))}')
    else:
        raise ValueError(f'unknown cloud {cloud}')


@main.command()
@click.option('--sync')
def create_data(sync):
    if sync:
        # how to sync for development, or pull the right version for
        # production
        # FIXME: what about AWS
        for vm in CONFIG['vms']:
            remote_sync_cmd = '(cd /home/ubuntu/test-copy; tar xf -)'
            run(['/bin/bash',
                 '-c',
                 "tar cf - create-test-copy-data.py"
                 f" | gcloud compute ssh ubuntu@test-copy-{vm['name']}-{PROFILE} --"
                 f" /bin/bash -c {shq(shq(remote_sync_cmd))}"])

    data_configs = CONFIG['data-configs']
    for location in CONFIG['locations']:
        path = url_join(location['path'], PROFILE)
        create_on = location['create-on']

        vm = find_by_key(CONFIG['vms'], 'name', create_on)

        for data_config in data_configs:
            config_path = url_join(path, data_config["name"])
            create_cmd = f'PYTHONPATH=/home/ubuntu/hail/hail/python python3 /home/ubuntu/test-copy/create-test-copy-data.py {shq(json.dumps(data_config))} {shq(config_path)}'
            _run_on_vm(vm, create_cmd)


@main.command()
def test():
    pass

if __name__ == '__main__':
    main()
