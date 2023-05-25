from invoke import task
import subprocess
import historical_price_updater.env as env
import yaml

image_name = 'price-updater'


@task
def install(c):
    """
    install requirements.txt
    :param c:
    :return:
    """
    print("Installing requirements.txt...")
    subprocess.run(["pip", "install", "-r", "requirements.txt"])


@task
def installdev(c):
    """
    install requirements-dev.txt and requirements.txt
    :param c:
    :return:
    """
    print("Installing requirements-dev.txt...")
    subprocess.run(["pip", "install", "-r", "requirements-dev.txt"])
    install(c)


@task
def dockerbuild(c):
    """
    run docker daemon if not already running and build the docker image
    :return:
    """
    print("Building docker image...")
    # print current working directory
    subprocess.run(["docker", "build", "-t", image_name, "-f", "docker/Dockerfile", "."])


@task
def dockertag(c):
    """
    tag the docker image
    :return:
    """
    docker_tag = f"{env.DOCKER_USERNAME}/{image_name}:latest"
    print("Tagging docker image...")
    subprocess.run(["docker", "tag", image_name, docker_tag])
    print(f"Tagged docker image: {docker_tag}")


@task
def dockerpush(c):
    """
    push the docker image to dockerhub
    :return:
    """
    dockerlogin(c)
    docker_tag = f"{env.DOCKER_USERNAME}/{image_name}:latest"
    print("Pushing docker image to DockerHub...")
    subprocess.run(["docker", "push", docker_tag])


@task
def dockerlogin(c):
    """
    login to dockerhub
    :return:
    """
    print("Logging into DockerHub...")
    subprocess.run(["docker", "login", "-u", env.DOCKER_USERNAME, "-p", env.DOCKER_TOKEN])


@task
def docker(ctx, b=False, t=False, p=False):
    """
    build, tag, and push the docker image to dockerhub
    usage: inv docker -b -t -p
    :return:
    """
    if b:
        dockerbuild(ctx)
    if t:
        dockertag(ctx)
    if p:
        dockerpush(ctx)


@task
def dockerpull(c):
    """
    pull the docker image from dockerhub
    :return:
    """
    print("Pulling docker image from DockerHub...")
    full_tag = f"{env.DOCKER_USERNAME}/{image_name}:latest"
    subprocess.run(["docker", "pull", full_tag])
    print(f"Pulled docker image: {full_tag}")


@task
def gcrdeploy(c):
    """
    Deploy the docker image to Google Cloud Run.
    :return:
    """
    print("Deploying docker image to Google Cloud Run...")
    tag = 'latest'
    docker_tag = f'docker.io/{env.DOCKER_USERNAME}/{image_name}:{tag}'
    envtoyaml(c)
    command = [
        'gcloud',
        'run',
        'deploy',
        image_name,
        '--image',
        docker_tag,
        '--region',
        'us-east1',
        '--no-allow-unauthenticated',
        '--project',
        env.GCR_PROJECT_ID,
        '--env-vars-file',
        './env.yaml'
    ]
    subprocess.run(command, check=True, shell=True)


@task
def envtoyaml(c):
    """
    Convert .env file to YAML file
    :param c:
    :return:
    """
    env_dict = {}
    # Usage example
    env_file = './.env'
    yaml_file = './env.yaml'

    # Read the .env file and populate the dictionary
    with open(env_file, 'r') as file:
        for line in file:
            line = line.strip()
            if line and not line.startswith('#'):
                key, value = line.split('=', 1)
                env_dict[key] = value

    # Write the dictionary to a YAML file
    with open(yaml_file, 'w') as file:
        yaml.dump(env_dict, file)

    print(f"YAML file '{yaml_file}' created successfully.")
