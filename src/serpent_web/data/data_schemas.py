from typing import TypeVar, Generic, Optional

from pydantic import BaseModel

from serpent_web.data.sql.base_sql_model import BaseSqlModel

TModel = TypeVar('TModel', bound=BaseSqlModel)


class PaginatedList(BaseModel, Generic[TModel]):
    total: int
    skip: int
    limit: int
    data: list[TModel]
    next: Optional[int]
    previous: Optional[int]
