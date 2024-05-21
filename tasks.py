from src.util_invoke_tasks import *
import env
import subprocess

@task
def updateKpdb(c):
    """
    Sync the local kpdb file with the remote kpdb file.
    :return:
    """
    print("Pulling latest kdbx file...")
    subprocess.run(['git', 'pull', env.KDBX_REPO], check=True)


@task
def gcrdeploy(c):
    """
    Deploy the docker image to Google Cloud Run.
    :return:
    """
    print("Deploying docker image to Google Cloud Run...")
    tag = 'latest'
    docker_username = get_env_var('DOCKER_USERNAME')
    image_name = get_env_var('IMAGE_NAME')
    docker_tag = f'docker.io/{docker_username}/{image_name}:{tag}'
    envtoyaml(c)
    command = [
        'gcloud',
        'run',
        'deploy',
        get_env_var('IMAGE_NAME'),
        '--image',
        docker_tag,
        '--region',
        'us-east1',
        '--no-allow-unauthenticated',
        '--project',
        get_env_var('GCR_PROJECT_ID'),
        '--env-vars-file',
        './env.yaml',
        '--memory',
        '4Gi',
    ]
    print(' '.join(command))
    subprocess.run(command, check=True, shell=True)
