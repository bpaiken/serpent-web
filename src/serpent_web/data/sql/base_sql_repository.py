import logging
from typing import TypeVar, Generic, Type, List, Dict, Any, get_args
from uuid import UUID

from pyexpat import model
from sqlalchemy.orm import Session

from serpent_web.data.sql.base_sql_model import BaseSqlModel

_logger = logging.getLogger(__name__)

TModel = TypeVar('TModel', bound=BaseSqlModel)  # orm model


class BaseSqlRepository(Generic[TModel]):
    def __init__(self, db: Session):
        self.model = self._get_base_model_type()
        self._db = db

    @classmethod
    def _get_base_model_type(cls) -> BaseSqlModel:
        bases = cls.__orig_bases__
        return get_args(bases[0])[0]

    def exists(self, query_filter: dict) -> bool:
        """
        Returns a boolean indicating if any object exists matching the specified filter.
        :param query_filter: dictionary of key-value pairs
        :return: boolean indicating if at least one matching object exists
        """
        model = self._db.query(self.model).get(query_filter)
        return model is not None

    def get_by_id(self, id: UUID) -> TModel:
        """
        Retrieve a single object as specified by its primary key.
        :param id: The model's primary key (e.g., 'id')
        :return: The model
        """
        return self._db.query(self.model).get(id)

    def get(self, query_filter: Dict[str, Any] = None, skip: int = 0, limit: int = 100) -> List[TModel]:
        """
        Retrieve multiple objects based on query filters, skip, and limit.
        :param query_filter: Dictionary of key-value pairs for filtering the query
        :param skip: Number of records to skip
        :param limit: Maximum number of records to return
        :return: List of models
        """
        query_filter = query_filter or {}
        query = self._db.query(self.model)
        if query_filter:
            for key, value in query_filter.items():
                if hasattr(self.model, key):
                    query = query.filter(getattr(self.model, key) == value)

        query = query.offset(skip).limit(limit)

        return query.all()

    def create(self, model: TModel, defer_commit: bool = False) -> TModel:
        """
        Create a new model instance and add it to the database.
        :param model: The model instance to create
        :param defer_commit: Whether to defer the commit of the transaction
        :return: The created model instance
        """
        self._db.add(model)
        self._handle_defer_commit_single_model(model, defer_commit)

        return model

    def update(self, model: TModel, defer_commit: bool = False) -> TModel:
        """
        Update an existing model instance in the database.
        :param model: The model instance to update
        :param defer_commit: Whether to defer the commit of the transaction
        :return: The updated model instance
        """
        if model.id is None:
            raise ValueError(f"The id of the existing model ({self.model.__name__}) is required for update action")

        if self.exists(model.id):
            self._db.add(model)
            self._handle_defer_commit_single_model(model, defer_commit)

            return model

        raise ValueError(f"Model of type {self.model.__name__} with id: {model.id} not found")

    def delete(self, id: UUID, defer_commit: bool = False) -> None:
        """
        Delete a single object as specified by its primary key.
        :param id: The model's primary key (e.g., 'id')
        :param defer_commit: Whether to defer the commit of the transaction
        :return: None
        """
        model = self._db.query(self.model).get(id)

        if model is not None:
            self._db.delete(model)
        if not defer_commit:
            self._db.commit()

    def _handle_defer_commit_single_model(self, model: TModel, defer_commit: bool) -> None:
        if defer_commit:
            self._db.flush()
        else:
            self._db.commit()
            self._db.refresh(model)
