from uuid import UUID
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, List

from serpent_web.data.sql.base_sql_repository import BaseSqlRepository

TModel = TypeVar('TModel', bound=object)


class BaseManager(Generic[TModel], ABC):
    """
    Abstract base class for managing database operations for a specific model.

    :param repository: The repository instance used for database operations.
    :type repository: BaseSqlRepository
    """
    _repository: BaseSqlRepository

    def __init__(self, repository: BaseSqlRepository):
        """
        Initialize the BaseManager with the provided repository.

        :param repository: The repository instance used for database operations.
        :type repository: BaseSqlRepository
        """
        self._repository = repository

    def get_by_id(self, id: UUID) -> TModel:
        """
        Retrieve a model instance by its primary key.

        :param id: The primary key of the model instance.
        :type id: UUID
        :return: The model instance corresponding to the given primary key.
        :rtype: TModel
        """
        return self._repository.get_by_id(id)

    def create(self, model: TModel, defer_commit: bool = False) -> TModel:
        """
        Create a new model instance in the database.

        :param model: The model instance to create.
        :type model: TModel
        :param defer_commit: Whether to defer the commit of the transaction.
        :type defer_commit: bool, optional
        :return: The created model instance.
        :rtype: TModel
        """
        return self._repository.create(model=model, defer_commit=defer_commit)

    def update(self, model: TModel, defer_commit: bool = False) -> TModel:
        """
        Update an existing model instance in the database.

        :param model: The model instance to update.
        :type model: TModel
        :param defer_commit: Whether to defer the commit of the transaction.
        :type defer_commit: bool, optional
        :return: The updated model instance.
        :rtype: TModel
        """
        return self._repository.update(model=model, defer_commit=defer_commit)

    def delete(self, id: UUID, defer_commit: bool = False) -> None:
        """
        Delete a model instance from the database by its primary key.

        :param id: The primary key of the model instance to delete.
        :type id: UUID
        :param defer_commit: Whether to defer the commit of the transaction.
        :type defer_commit: bool, optional
        :return: None
        """
        self._repository.delete(id=id, defer_commit=defer_commit)

    def get(self, query_filter: dict[str, any] = None, skip: int = 0, limit: int = 100) -> list[TModel]:
        """
        Retrieve multiple objects based on query filters, skip, and limit.
        :param query_filter: Dictionary of key-value pairs for filtering the query
        :param skip: Number of records to skip
        :param limit: Maximum number of records to return
        :return: List of models
        """
        return self._repository.get(query_filter=query_filter, skip=skip, limit=limit)
