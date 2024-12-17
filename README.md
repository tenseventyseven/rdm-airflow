# rdm-airflow

## Local setup

After checking out the repo:

```sh
# Create venv
python -m venv venv

# Activate venv
source venv/bin/activate

# Install requirements
pip install -r requirements.txt

# Set environment variables in .rc
export AIRFLOW_HOME=~/path/to/rdm-airflow
export AIRFLOW_VERSION=2.10.4

# Create .env with UID
AIRFLOW_UID=<uid>

# Start basic airflow
airflow standalone

# Credentials
cat standalone_admin_password.txt

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
