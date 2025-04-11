from prefect_dbt.cli.commands import DbtCoreOperation
from prefect import flow


@flow
def trigger_dbt_flow() -> str:
    result = DbtCoreOperation(
        commands=["dbt build --vars '{'is_test_run': false}'"],
        project_dir="../dbt",
        profiles_dir="~/dbt",
        overwrite_profiles=True,
    ).run()
    return result


if __name__ == "__main__":
    trigger_dbt_flow()
