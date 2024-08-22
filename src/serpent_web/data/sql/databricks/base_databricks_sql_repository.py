# import logging
# from typing import TypeVar, Generic, Type, List, Dict, Any
# from uuid import UUID
#
# from starlette.exceptions import HTTPException
# from pydantic import BaseModel
# from sqlalchemy.orm import Session
#
# from serpent_web.data.sql.databricks.base_databricks_sql_model import BaseDatabricksSqlModel
# from serpent_web.data.sql.base_schema import BaseSchema
# from serpent_web.data.sql.pydantic_helpers import parse_pydantic_schema
#
# _logger = logging.getLogger("app")
#
# TModel = TypeVar('TModel', bound=BaseDatabricksSqlModel)  # orm model
# TSchema = TypeVar('TSchema', bound=BaseSchema)  # pydantic schema
# TCreateSchema = TypeVar('TCreateSchema', bound=BaseModel)  # pydantic schema
#
#
# class BaseDatabricksSqlRepository(Generic[TModel, TSchema, TCreateSchema]):
#     def __init__(self, model: Type[TModel], schema: Type[TSchema], create_schema: Type[TCreateSchema]):
#         self.model = model
#         self.schema = schema
#
#     def exists(self, db: Session, filter: dict) -> bool:
#         """
#         Returns a boolean indicating if any object exists matching the specified filter
#         :param db: SqlAlchemy database session
#         :param filter: dictionary of key value pairs
#         :return: boolean indicating at least one matching object exists
#         """
#         model = db.query(self.model).get(filter)
#         if model:
#             return True
#
#         return False
#
#     def get_by_id(self, db: Session, pk: UUID) -> TModel:
#         """
#         Retrieve a single object as specified by its primary key.
#         :param db: The database session
#         :param pk: The models primary key (e.g. 'id')
#         :return: The model
#         """
#         _logger.info(f"Querying model by id of type: {self.model.__name__} and with id: {pk}")
#         model = db.query(self.model).get(pk)
#         return model
#
#     def get(self, db: Session, filter: Dict[str, Any] = None, skip: int = 0, limit: int = 100) -> List[TModel]:
#         filter = filter or {}
#
#         _logger.info(f"Querying model of type: {self.model.__name__}")
#         query = db.query(self.model).offset(skip).limit(limit)
#         if filter:
#             for key, value in filter.items():
#                 if hasattr(self.model, key):
#                     query = query.filter(getattr(self.model, key) == value)
#
#         return query.all()
#
#     def create(self, db: Session, schema: TCreateSchema, defer_commit: bool = False) -> TModel:
#         parsed_schema = parse_pydantic_schema(schema)
#         model = self._create_model_instance(parsed_schema)
#
#         _logger.info(f"Creating model of type: {self.model.__name__}")
#         db.add(model)
#         if defer_commit is False:
#             db.commit()
#             db.refresh(model)
#         else:
#             db.flush()
#
#         return model
#
#     def update(self, db: Session, schema: TSchema, defer_commit: bool = False) -> TModel:
#         model = db.query(self.model).get(schema.pk)
#         if model is None:
#             raise HTTPException(status_code=404, detail=f"Item with ID {schema.pk} not found")
#
#         # TODO update w/ pydantic_parse if required
#         schema_dict = schema.model_dump()
#         for key, value in schema_dict.items():
#             setattr(model, key, value)
#
#         _logger.info(f"Updating model (id: {model.id}) of type: {self.model.__name__}")
#         db.add(model)
#         if defer_commit is False:
#             db.commit()
#             db.refresh(model)
#         else:
#             db.flush()
#
#         return model
#
#     def save(self, db: Session, schema: TSchema | TCreateSchema, defer_commit: bool = False):
#         existing_model = None
#         if "id" in schema.model_fields:
#             existing_model = db.query(self.model).get(schema.id)
#
#         if existing_model is None:
#             return self.create(db, schema, defer_commit)
#         else:
#             return self.update(db, schema, defer_commit)
#
#     def delete(self, db: Session, pk: UUID, defer_commit: bool = False) -> None:
#         """
#         Delete a single object as specified by its primary key.
#         :param db: The database session
#         :param pk: The models primary key (e.g. 'id')
#         :param defer_commit:
#         :return: None
#         """
#         model = db.query(self.model).get(pk)
#
#         if model is not None:
#             _logger.info(f"Deleting model with id: {model.id} or model was not found")
#             db.delete(model)
#
#         if defer_commit is False:
#             db.commit()
#
#     def _create_model_instance(self, data) -> TModel:
#         # Filter the data to only include fields present in the model
#         model_fields = {column.name for column in self.model.__table__.columns}
#         filtered_data = {key: value for key, value in data.items() if key in model_fields}
#
#         # Create an instance of the model
#         instance = self.model(**filtered_data)
#         return instance
