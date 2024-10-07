import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from utils.shell import execute_command
from merscope.include.models import TransferRequestModel
import logging

SRC_ROOT_FOLDER = "/cygdrive/d/MERSCOPEDATA"
DEST_ROOT_FOLDER = "/Users/u.ja/scratch/merscope"


logger = logging.getLogger(__name__)


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["merscope"],
    params={
        "projectId": "my_project",
        "sessionId": "my_session",
        "submitter": "me",
        "recipient": "you",
        "srcFolder": "202405241031_20240524G000402ProteinVeri_VMSC04501",
        "destFolder": "my_dataset",
        "notes": "Here are some notes",
    },  # Matches TransferRequestModel
)
def merscope_transfer_dag():
    @task
    def validate_request(**kwargs) -> TransferRequestModel:
        # Is the shape of the request correct?
        transfer_request = TransferRequestModel(**kwargs["params"])

        return transfer_request

    @task
    def find_directories():
        ssh_hook = SSHHook(ssh_conn_id="merscope_ssh_conn")
        ssh_client = ssh_hook.get_conn()

        # Find all first-level directories in the MERSCOPEDATA folder
        find_command = rf"find {SRC_ROOT_FOLDER} -mindepth 1 -maxdepth 1 -type d -exec basename {{}} \;"

        _, stdout, stderr = ssh_client.exec_command(find_command)

        directories = stdout.read().decode().strip().splitlines()
        errors = stderr.read().decode().strip()

        if errors:
            raise RuntimeError(f"Error running find command: {errors}")

        return directories

    @task
    def perform_rsync(transfer_request: TransferRequestModel):
        connection = BaseHook.get_connection(conn_id="merscope_ssh_conn")
        dest_root = Variable.get("merscope_dest_root", default_var=DEST_ROOT_FOLDER)

        remote_user = connection.login
        remote_host = connection.host
        private_key = connection.extra_dejson.get("key_file", None)

        src_path = f"{SRC_ROOT_FOLDER}/{transfer_request.srcFolder}"
        dest_path = f"{dest_root}/{transfer_request.destFolder}"

        rsync_command = (
            "rsync "
            "-avhzP "  # Archive mode, verbose, human-readable, compress, partial progress
            "--progress "  # Show progress during transfer
            "--partial "  # Keep partially transferred files
            "--inplace "  # Update destination files in-place
            "--ignore-existing "  # Skip files that already exist on the destination
            "--max-size=10MB "  # Maximum file size to transfer - set this for debugging only, remove otherwise
            f'-e "ssh -i {private_key}" '  # SSH with private key
            f"{remote_user}@{remote_host}:{src_path} "  # Source path
            f"{dest_path}"  # Destination path
        )
        execute_command(rsync_command, logger)

    find_directories()
    # TODO: actually validate the request against the directories
    transfer_request = validate_request()

    return (
        EmptyOperator(task_id="start")
        >> transfer_request
        >> perform_rsync(transfer_request)  # type: ignore
        >> EmptyOperator(task_id="end")
    )


merscope_transfer_dag()
