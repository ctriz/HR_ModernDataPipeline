# build_run.ps1

# Build the Airflow image
docker build -t airflow-minimal .

# Run Airflow container
docker run -d `
  -p 8080:8080 `
  -v ${PWD}\dags:/opt/airflow/dags `
  -v ${PWD}\logs:/opt/airflow/logs `
  -v ${PWD}\plugins:/opt/airflow/plugins `
  --name airflow-local `
  airflow-minimal standalone
