import uuid

from sqlalchemy import Column, DateTime, event, String
from sqlalchemy.orm import declared_attr, as_declarative

from serpent_web.core.util.datetime_helpers import utc_now_time_aware
from serpent_web.core.util.string_helpers import snake_to_camel, title_to_snake


@as_declarative()
class BaseSqliteModel:
    @declared_attr
    def __tablename__(cls):
        return title_to_snake(cls.__name__)

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    created_on = Column(DateTime, default=utc_now_time_aware)
    updated_on = Column(DateTime, default=utc_now_time_aware, onupdate=utc_now_time_aware)

    @property
    def pk(self):  # alias
        return self.id

    @property
    def timestamp(self):  # alias
        return self.created_on

    class Config:
        alias_generator = snake_to_camel


# Event listener to update `updated_on` before update
@event.listens_for(BaseSqliteModel, 'before_update', propagate=True)
def update_timestamp(mapper, connection, target):
    target.updated_on = utc_now_time_aware()
