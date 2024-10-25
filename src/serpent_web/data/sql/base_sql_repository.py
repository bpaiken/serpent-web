import logging
from typing import TypeVar, Generic, Type, List, Dict, Any, get_args
from uuid import UUID

from pyexpat import model

from sqlalchemy import String, Text, or_
from sqlalchemy.orm import Session

from serpent_web.data.data_schemas import PaginatedList
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

    def get_models_by_ids(self, ids: list[any]) -> list[TModel]:
        """
        Retrieve multiple objects as specified by their primary keys.
        :param ids: a list of the model's primary keys (e.g., 'id')
        :return: a list of the models
        """
        return self._db.query(self.model).filter(self.model.id.in_(ids)).all()

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

    def get_paginated(
            self,
            query_filter: Dict[str, Any] = None,
            skip: int = None,
            limit: int = None,
            order_by: List[str] = None,
            search_fields: List[str] = None,
            search_text: str = None
    ) -> PaginatedList[TModel]:
        """
        Retrieve multiple objects based on query filters, skip, limit, order, and search.
        :param query_filter: Dictionary of key-value pairs for filtering the query
        :param skip: Number of records to skip (optional)
        :param limit: Maximum number of records to return (optional)
        :param order_by: List of field names to order by. Use '-' prefix for descending order.
        :param search_fields: List of string or text field names to search
        :param search_text: Text to search for in the specified fields
        :return: Paginated response of models
        """
        query_filter = query_filter or {}
        base_query = self._db.query(self.model)

        # Apply query filters
        if query_filter:
            for key, value in query_filter.items():
                if hasattr(self.model, key):
                    base_query = base_query.filter(getattr(self.model, key) == value)

        # Apply search filters
        if search_text and search_fields:
            search_conditions = []
            for field_name in search_fields:
                if hasattr(self.model, field_name):
                    column = getattr(self.model, field_name)
                    # Get the column type
                    column_type = self.model.__table__.columns[field_name].type
                    if not isinstance(column_type, (String, Text)):
                        raise TypeError(f"Field '{field_name}' is not a string or text column")
                    search_conditions.append(column.ilike(f"%{search_text}%"))
                else:
                    raise AttributeError(f"'{self.model.__name__}' has no attribute '{field_name}'")
            if search_conditions:
                base_query = base_query.filter(or_(*search_conditions))

        # Apply sorting if order_by is provided
        if order_by:
            order_criteria = []
            for field_name in order_by:
                descending = False
                if field_name.startswith('-'):
                    descending = True
                    field_name = field_name[1:]
                if hasattr(self.model, field_name):
                    field = getattr(self.model, field_name)
                    if descending:
                        order_criteria.append(field.desc())
                    else:
                        order_criteria.append(field.asc())
                else:
                    raise AttributeError(f"'{self.model.__name__}' has no attribute '{field_name}'")
            if order_criteria:
                base_query = base_query.order_by(*order_criteria)

        # Calculate total number of items before pagination
        total = base_query.count()

        # Apply pagination if skip or limit is provided
        if skip is not None:
            base_query = base_query.offset(skip)
        if limit is not None:
            base_query = base_query.limit(limit)

        data = base_query.all()

        # Calculate next_page and previous_page
        if limit is not None:
            if skip is None:
                skip = 0
            if skip + limit < total:
                next_page = (skip + limit) // limit + 1
            else:
                next_page = None
            if skip > 0:
                previous_page = max((skip - limit) // limit + 1, 1)
            else:
                previous_page = None
        else:
            next_page = None
            previous_page = None

        # Construct and return the PaginatedResponse
        return PaginatedList[TModel](
            total=total,
            skip=skip if skip is not None else 0,
            limit=limit if limit is not None else total,
            data=data,
            next=next_page,
            previous=previous_page
        )

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
