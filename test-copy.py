import os
import subprocess as sp
from shlex import quote as shq
import asyncio
import json
import urllib.parse
import yaml
import click
from concurrent.futures import ThreadPoolExecutor
from hailtop.aiotools.fs import RouterAsyncFS, LocalAsyncFS
from hailtop.aiogoogle import GoogleStorageAsyncFS
from hailtop.aiotools.s3asyncfs import S3AsyncFS


def url_join(url, path):
    parsed = urllib.parse.urlparse(url)
    return urllib.parse.urlunparse(parsed._replace(path=os.path.join(parsed.path, path)))


def run(args, **kwargs):
    assert 'shell' not in kwargs
    kwargs['shell'] = True
    if 'check' not in kwargs:
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


def _create_vms():
    for vm in CONFIG['vms']:
        _create_vm(vm)


@main.command()
def create_vms():
    _create_vms()


def _delete_vm(vm):
    name = vm['name']
    run(subst(vm['delete'], '__NAME__', f'test-copy-{name}-{PROFILE}'), check=False)


@main.command()
@click.argument('name')
def delete_vm(name):
    vm = find_by_key(CONFIG['vms'], 'name', name)
    _delete_vm(vm)


def _delete_vms():
    for vm in CONFIG['vms']:
        _delete_vm(vm)


@main.command()
def delete_vms():
    _delete_vms()


def _run_on_vm(vm, cmd):
    cloud = vm['cloud']
    vm_name = f"test-copy-{vm['name']}-{PROFILE}"
    if cloud == 'gcp':
        run(f'gcloud compute ssh ubuntu@{vm_name} -- /bin/bash -c {shq(shq(cmd))}')
    elif cloud == 'aws':
        id_file = CONFIG['aws-id-file']
        ip_cmd = f"aws --no-cli-pager ec2 describe-instances --region us-east-1 --filter Name=tag:Name,Values={vm_name} Name=instance-state-name,Values=pending,running | jq -r '.Reservations[0].Instances[0].PublicIpAddress'"
        run(f'ssh -o "StrictHostKeyChecking no" -i {id_file} ubuntu@$({ip_cmd}) -- /bin/bash -c {shq(shq(cmd))}')
    else:
        raise ValueError(f'unknown cloud {cloud}')


def _scp_vm(vm, filespec):
    cloud = vm['cloud']
    vm_name = f"test-copy-{vm['name']}-{PROFILE}"
    if cloud == 'gcp':
        filespec = filespec.replace('__HOST__', vm_name)
        run(f'gcloud compute scp {filespec}')
    elif cloud == 'aws':
        id_file = CONFIG['aws-id-file']
        ip_cmd = f"aws --no-cli-pager ec2 describe-instances --region us-east-1 --filter Name=tag:Name,Values={vm_name} Name=instance-state-name,Values=pending,running | jq -r '.Reservations[0].Instances[0].PublicIpAddress'"
        filespec = filespec.replace('__HOST__', f'$({ip_cmd})')
        run(f'scp -o "StrictHostKeyChecking no" -i {id_file} {filespec}')
    else:
        raise ValueError(f'unknown cloud {cloud}')


def _copy_to_vm(vm, src, dest):
    _scp_vm(vm, f'{src} ubuntu@__HOST__:{dest}')


def _copy_from_vm(vm, src, dest):
    _scp_vm(vm, f'ubuntu@__HOST__:{src} {dest}')


def _create_data():
    vm_names = set()
    for location in CONFIG['locations']:
        create_on = location['create-on']
        vm_names.add(create_on)

    for vm_name in vm_names:
        vm = find_by_key(CONFIG['vms'], 'name', vm_name)

        _copy_to_vm(vm, './create-test-copy-data.py', '/home/ubuntu/create-test-copy-data.py')

        git_config = CONFIG['git']
        org = git_config['org']
        repo = git_config['repo']
        version = git_config['version']

        _run_on_vm(vm, f'''
cd /home/ubuntu/hail
git remote remove {org} || true
git remote add {org} https://github.com/{org}/{repo}.git
git fetch --all
git checkout {version}
''')

    for location in CONFIG['locations']:
        path = url_join(location['path'], PROFILE)
        create_on = location['create-on']

        vm = find_by_key(CONFIG['vms'], 'name', create_on)

        for data_config in CONFIG['data-configs']:
            config_path = url_join(path, data_config["name"])
            create_cmd = f'PYTHONPATH=/home/ubuntu/hail/hail/python time python3 /home/ubuntu/create-test-copy-data.py {shq(json.dumps(data_config))} {shq(config_path)}'
            _run_on_vm(vm, create_cmd)


@main.command()
def create_data():
    _create_data()


def _test_copy():
    vm_names = set()
    for case in CONFIG['cases']:
        run_on = case['run-on']
        vm_names.add(run_on)

    for vm_name in vm_names:
        vm = find_by_key(CONFIG['vms'], 'name', vm_name)

        _copy_to_vm(vm, './benchmark-copy.py', '/home/ubuntu/benchmark-copy.py')

        git_config = CONFIG['git']
        org = git_config['org']
        repo = git_config['repo']
        version = git_config['version']

        _run_on_vm(vm, f'''
cd /home/ubuntu/hail
git remote remove {org} || true
git remote add {org} https://github.com/{org}/{repo}.git
git fetch --all
git checkout {version}
''')

    results = []
    data_configs = CONFIG['data-configs']
    for case in CONFIG['cases']:
        from_path = url_join(case['from'], PROFILE)
        to_path = url_join(case['to'], PROFILE)
        run_on = case['run-on']

        vm = find_by_key(CONFIG['vms'], 'name', run_on)

        for data_config in data_configs:
            config_name = data_config['name']
            config_from = url_join(from_path, config_name)
            config_to = url_join(to_path, config_name)

            benchmark_cmd = f'PYTHONPATH=/home/ubuntu/hail/hail/python python3 /home/ubuntu/benchmark-copy.py {CONFIG["replicas"]} {shq(config_from)} {shq(config_to)}'
            _run_on_vm(vm, benchmark_cmd)

            _copy_from_vm(vm, '/home/ubuntu/times.json', 'times.json')
            with open('times.json', 'r') as f:
                times = json.loads(f.read())
            results.append({
                'config': config_name,
                'from': config_from,
                'to': config_to,
                'times': times
            })

    print(results)
    with open('results.json', 'w') as f:
        f.write(json.dumps(results))


@main.command()
def test_copy():
    _test_copy()


async def cleanup_locations():
    with ThreadPoolExecutor() as thread_pool:
        async with RouterAsyncFS('file', [LocalAsyncFS(thread_pool),
                                          GoogleStorageAsyncFS(),
                                          S3AsyncFS(thread_pool)]) as fs:
            sema = asyncio.Semaphore(15)

            for location in CONFIG['locations']:
                path = url_join(location['path'], PROFILE)

                print(path)

                parsed = urllib.parse.urlparse(path)
                if not parsed.scheme or parsed.scheme == 'file':
                    continue;

                await fs.rmtree(sema, path)


def _cleanup():
    _delete_vms()

    asyncio.run(cleanup_locations())


@main.command()
def cleanup():
    _cleanup()


@main.command()
@click.option('--cleanup/--no-cleanup', default=True)
def all(cleanup):
    _create_vms()
    _create_data()
    _test_copy()
    if cleanup:
        _cleanup()


if __name__ == '__main__':
    main()
