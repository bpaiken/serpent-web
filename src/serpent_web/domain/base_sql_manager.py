from abc import ABC
from typing import TypeVar

from serpent_web.data.sql.base_sql_model import BaseSqlModel
from serpent_web.domain.base_manager import BaseManager

TModel = TypeVar('TModel', bound=BaseSqlModel)


class BaseSqlManager(BaseManager[TModel], ABC):
    pass
