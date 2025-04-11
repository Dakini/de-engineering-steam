from prefect import flow, task
import subprocess

@task
def run_dbt_command(command: str):
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"DBT command failed: {result.stderr}")

@flow
def dbt_flow():
    run_dbt_command("cd ../dbt && dbt build --vars '{'is_test_run': false}'")


if __name__ == "__main__":
    dbt_flow()