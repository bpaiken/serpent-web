# import uuid
#
# from sqlalchemy import Column, Uuid
# from sqlalchemy.orm import declared_attr, as_declarative
# from databricks.sqlalchemy import TIMESTAMP
#
# from serpent_web.core.util.datetime_helpers import utc_now_time_aware
# from serpent_web.core.util.string_helpers import snake_to_camel, title_to_snake
#
#
# @as_declarative()
# class BaseDatabricksSqlModel:
#     @declared_attr
#     def __tablename__(cls):
#         return title_to_snake(cls.__name__)
#
#     id = Column(Uuid, primary_key=True, default=uuid.uuid4)
#     created_on = Column(TIMESTAMP, default=utc_now_time_aware)
#
#     @property
#     def timestamp(self):  # alias
#         return self.created_on
#
#     class Config:
#         alias_generator = snake_to_camel
