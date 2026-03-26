from dsp.shared.config import config_item
from dsp.shared.logger import log_action, LogLevel
from dsp.shared.models import DatabricksUser
from dsp.shared.store.base import BaseStore
from dsp.shared.aws import dynamodb_retry_backoff


class DatabricksUsersStore(BaseStore[DatabricksUser]):
    _table_name = "databricks_users"
    _keys = ["user_name", "user_id"]
    _groups_dict = None
    _databricks_users = None

    def __init__(self, table=None, blocks_store=None):
        super().__init__(DatabricksUser, table)

    @log_action(log_level=LogLevel.INFO)
    @dynamodb_retry_backoff()
    def update_table(self, user_list: dict) -> int:
        for user, groups in user_list.items():
            user_id, user_name = user.lower().split("_", 1)
            item = DatabricksUser(dict(
                user_name=user_name,
                user_id=user_id,
                users_groups=groups
            ))

            self.put(item)

        return self.table.item_count
