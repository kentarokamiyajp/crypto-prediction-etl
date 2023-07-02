import subprocess


def hello_world():
    return "hello world"


def create_file():
    subprocess.run("touch /opt/airflow/data/sample.txt", shell=True)
    output_str = subprocess.run("ls -al /opt/airflow", capture_output=True, text=True, shell=True).stdout
    return output_str


def delete_file():
    subprocess.run("rm /opt/airflow/data/sample.txt", shell=True)
    output_str = subprocess.run("ls -al /opt/airflow", capture_output=True, text=True, shell=True).stdout
    return output_str


def check_hostname():
    output_str = subprocess.run("echo `hostname`", capture_output=True, text=True, shell=True).stdout
    return output_str


def check_whoami():
    output_str = subprocess.run("echo `whoami`", capture_output=True, text=True, shell=True).stdout
    return output_str


def check_pwd():
    output_str = subprocess.run("echo `pwd`", capture_output=True, text=True, shell=True).stdout
    return output_str
