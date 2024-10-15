from typing import TypeVar, Generic, Optional

from pydantic import BaseModel

TModel = TypeVar('TModel')


class PaginatedList(BaseModel, Generic[TModel]):
    total: int
    skip: int
    limit: int
    data: list[TModel]
    next: Optional[int]
    previous: Optional[int]

    class Config:
        arbitrary_types_allowed = True
