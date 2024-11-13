from abc import ABC, abstractmethod
from abc import ABCMeta
from typing import TypeVar, Generic

from sqlalchemy import Column, event
from sqlalchemy.orm import declared_attr, as_declarative, DeclarativeMeta, Mapped
from sqlalchemy.types import TIMESTAMP

from serpent_web.core.util.datetime_helpers import utc_now_time_aware
from serpent_web.core.util.string_helpers import snake_to_camel, title_to_snake

TId = TypeVar('TId', bound=object)


class BaseMeta(DeclarativeMeta, ABCMeta):
    pass


@as_declarative(metaclass=BaseMeta)
class BaseSqlModel(Generic[TId], ABC):
    @declared_attr
    def __tablename__(cls):
        return title_to_snake(cls.__name__)

    @declared_attr
    def id(cls) -> Mapped[TId]:
        return Column(cls.id_model_type(), primary_key=True, default=cls.default_id())

    created_on = Column(TIMESTAMP(timezone=True), default=utc_now_time_aware)
    updated_on = Column(TIMESTAMP(timezone=True), default=utc_now_time_aware, onupdate=utc_now_time_aware)

    @property
    def pk(self):  # alias
        return self.id

    @property
    def timestamp(self):  # alias
        return self.created_on

    @classmethod
    @abstractmethod
    def id_model_type(cls):
        pass

    @classmethod
    @abstractmethod
    def default_id(cls):
        pass

    class Config:
        alias_generator = snake_to_camel


# Event listener to update `updated_on` before update
@event.listens_for(BaseSqlModel, 'before_update', propagate=True)
def update_timestamp(mapper, connection, target):
    target.updated_on = utc_now_time_aware()
