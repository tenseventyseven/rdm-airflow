# rdm-airflow

## Local setup

After checking out the repo:

```sh
# Setup venv
python -m venv .venv && source .venv/bin/activate && python -m pip install --upgrade pip

# Install requirements
pip install -r requirements.txt

# Set environment variables in .bashrc
export AIRFLOW_HOME=~/path/to/rdm-airflow
export AIRFLOW_VERSION=2.10.4

# Create .env with UID
echo "AIRFLOW_UID=$(id -u $USER)" > .env

# Extra directories
mkdir -p {data,secrets}

# Start basic airflow
AIRFLOW__CORE__LOAD_EXAMPLES=false airflow standalone airflow standalone

# Login to <hostname>:8080 with credentials printed above (and stored in standalone_admin_password.txt)
# Try triggering the `hello_world` example DAG

# Exit basic airflow
ctrl-c

# Update settings in generated airflow.cfg
load_examples = False
auth_backends = airflow.providers.fab.auth_manager.api.auth.backend.basic_auth
expose_config = True
# ...CORS
access_control_allow_headers = *
access_control_allow_methods = *
access_control_allow_origins = *
# ...to support Pydantic *Model deserialisation
allowed_deserialization_classes_regexp = .*Model

# Restart airflow to pip up config changes
```

## Local setup with containers

```sh
# Bring up services
podman compose -f docker-compose-services.yaml up

```

## Add connections

```sh
# Add merscope SSH connection
MERSCOPE_HOST="10.1.14.21"
MERSCOPE_USER="Merscope3"
MERSCOPE_PRIVATE_KEY="/path/to/merscope_id_ed25519"
airflow connections add merscope_ssh_conn \
    --conn-type ssh \
    --conn-host $MERSCOPE_HOST \
    --conn-login ${MERSCOPE_USER} \
    --conn-extra "{\"key_file\": \"$MERSCOPE_PRIVATE_KEY\", \"no_host_key_check\": true}"
```
