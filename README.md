# rdm-airflow

## Local setup
After checking out the repo:

```sh
# Create venv
python -m venv .venv

# Activate venv
source .venv/bin/activate

# Install requirements
pip install -r requirements.txt

# Start basic airflow
airflow standalone

# Credentials
cat standalone_admin_password.txt

# Set environment variables in .rc
export AIRFLOW_HOME=~/path/to/rdm-airflow
export AIRFLOW_VERSION=2.10.2

# Create .env with UID
AIRFLOW_UID=<uid>

# Update settings in generated airflow.cfg
load_examples = False
auth_backends = airflow.providers.fab.auth_manager.api.auth.backend.basic_auth
access_control_allow_headers = *
access_control_allow_methods = *
access_control_allow_origins = *
```
