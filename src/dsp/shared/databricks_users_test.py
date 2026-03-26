from collections import defaultdict


def test_update_table(temp_databricks_users_store):
    databricks_users = defaultdict(list, {
      "100001_dae_databricks_user@nhs.net": ["dae-databricks", "data-managers", "db_owners_yanai_build"],
      "100002_ops_user@nhs.net": ["ops"],
      "100003_admin_user@nhs.net": ["admins", "ops"]})
    store = temp_databricks_users_store
    result = store.update_table(databricks_users)
    assert 3 == result
