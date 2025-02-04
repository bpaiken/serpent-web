import logging
from typing import TypeVar, Generic, Type, List, Dict, Any, get_args, Optional, Tuple
from uuid import UUID

from pyexpat import model

from sqlalchemy import String, Text, or_, select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session, RelationshipProperty

from serpent_web.data.data_schemas import PaginatedList
from serpent_web.data.sql.base_sql_model import BaseSqlModel

_logger = logging.getLogger(__name__)

TModel = TypeVar('TModel', bound=BaseSqlModel)  # orm model


class BaseSqlRepository(Generic[TModel]):
    def __init__(self, db: Session | AsyncSession):
        self.model = self._get_base_model_type()
        self._db = db

    @classmethod
    def _get_base_model_type(cls) -> BaseSqlModel:
        bases = cls.__orig_bases__
        return get_args(bases[0])[0]

    # ─── SYNCHRONOUS EXISTS ───────────────────────────────────────────────
    def exists(self, query_filter: dict) -> bool:
        """
        Returns a boolean indicating if any object exists matching the specified filter.
        :param query_filter: dictionary of key-value pairs
        :return: boolean indicating if at least one matching object exists
        """
        stmt = select(self.model).filter_by(**query_filter)
        result = self._db.execute(stmt)
        return result.scalar() is not None


    # ─── ASYNCHRONOUS EXISTS ───────────────────────────────────────────────
    async def exists_async(self, query_filter: dict) -> bool:
        """
        Returns a boolean indicating if any object exists matching the specified filter.
        :param query_filter: dictionary of key-value pairs
        :return: boolean indicating if at least one matching object exists
        """

        stmt = select(self.model).filter_by(**query_filter)
        result = await self._db.execute(stmt)
        return result.scalar() is not None


    # ─── SYNCHRONOUS GET_BY_ID ───────────────────────────────────────────────
    def get_by_id(self, id: UUID) -> Optional[TModel]:
        """
        Retrieve a single object as specified by its primary key.
        :param id: The model's primary key (e.g., 'id')
        :return: The model
        """
        return self._db.query(self.model).get(id)


    # ─── ASYNCHRONOUS GET_BY_ID ───────────────────────────────────────────────
    async def get_by_id_async(self, id: UUID) -> Optional[TModel]:
        """
        Retrieve a single object as specified by its primary key.
        :param id: The model's primary key (e.g., 'id')
        :return: The model
        """
        return await self._db.get(self.model, id)


    # ─── SYNCHRONOUS GET_BY_IDS ───────────────────────────────────────────────
    def get_models_by_ids(self, ids: list[any]) -> list[TModel]:
        """
        Retrieve multiple objects as specified by their primary keys.
        :param ids: a list of the model's primary keys (e.g., 'id')
        :return: a list of the models
        """
        return self._db.query(self.model).filter(self.model.id.in_(ids)).all()


    # ─── ASYNCHRONOUS GET_BY_IDS ───────────────────────────────────────────────
    async def get_models_by_ids_async(self, ids: list[any]) -> list[TModel]:
        """
        Retrieve multiple objects as specified by their primary keys.
        :param ids: a list of the model's primary keys (e.g., 'id')
        :return: a list of the models
        """
        stmt = select(self.model).filter(self.model.id.in_(ids))
        result = await self._db.execute(stmt)
        return list(result.scalars())


    # ─── SYNCHRONOUS GET ───────────────────────────────────────────────
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


    # ─── ASYNCHRONOUS GET ───────────────────────────────────────────────
    async def get_async(self, query_filter: Dict[str, Any] = None, skip: int = 0, limit: int = 100) -> List[TModel]:
        """
        Retrieve multiple objects based on query filters, skip, and limit.

        :param query_filter: Dictionary of key-value pairs for filtering the query.
        :param skip: Number of records to skip.
        :param limit: Maximum number of records to return.
        :return: List of model instances.
        """
        query_filter = query_filter or {}
        stmt = select(self.model)

        # Apply dynamic filtering based on provided key-value pairs.
        for key, value in query_filter.items():
            if hasattr(self.model, key):
                stmt = stmt.filter(getattr(self.model, key) == value)

        stmt = stmt.offset(skip).limit(limit)
        result = await self._db.execute(stmt)
        return list(result.scalars())


    # ─── SYNCHRONOUS PAGINATION ───────────────────────────────────────────────
    def get_paginated(
            self,
            query_filter: Optional[Dict[str, Any]] = None,
            skip: int = None,
            limit: int = None,
            order_by: Optional[List[str]] = None,
            search_fields: Optional[List[str]] = None,
            search_text: Optional[str] = None
    ) -> PaginatedList[TModel]:
        """
        Retrieve multiple objects (synchronously) based on filters, pagination, ordering, and search.
        """
        # Build the common base statement.
        stmt = self._build_base_paginated_get_stmt(query_filter, order_by, search_fields, search_text)

        # Build and execute a count query (removing ordering).
        count_stmt = stmt.with_only_columns(func.count()).order_by(None)
        count_result = self._db.execute(count_stmt)
        total = count_result.scalar()  # total number of matching records

        # Apply pagination (offset and limit).
        if skip is not None:
            stmt = stmt.offset(skip)
        if limit is not None:
            stmt = stmt.limit(limit)

        result = self._db.execute(stmt)
        data = result.unique().scalars().all()

        next_page, previous_page = self._calculate_pagination(total, skip, limit)
        return PaginatedList[TModel](
            total=total,
            skip=skip if skip is not None else 0,
            limit=limit if limit is not None else total,
            data=data,
            next=next_page,
            previous=previous_page
        )


    # ─── ASYNCHRONOUS PAGINATION ───────────────────────────────────────────────
    async def get_paginated_async(
            self,
            query_filter: Optional[Dict[str, Any]] = None,
            skip: int = None,
            limit: int = None,
            order_by: Optional[List[str]] = None,
            search_fields: Optional[List[str]] = None,
            search_text: Optional[str] = None
    ) -> PaginatedList[TModel]:
        """
        Retrieve multiple objects (asynchronously) based on filters, pagination, ordering, and search.
        """
        # Build the common base statement.
        stmt = self._build_base_paginated_get_stmt(query_filter, order_by, search_fields, search_text)

        # Build and execute a count query asynchronously.
        count_stmt = stmt.with_only_columns(func.count()).order_by(None)
        count_result = await self._db.execute(count_stmt)
        total = count_result.scalar_one()  # total number of matching records

        # Apply pagination.
        if skip is not None:
            stmt = stmt.offset(skip)
        if limit is not None:
            stmt = stmt.limit(limit)

        result = await self._db.execute(stmt)
        data = result.unique().scalars().all()

        next_page, previous_page = self._calculate_pagination(total, skip, limit)
        return PaginatedList[TModel](
            total=total,
            skip=skip if skip is not None else 0,
            limit=limit if limit is not None else total,
            data=data,
            next=next_page,
            previous=previous_page
        )


    # ─── SYNCHRONOUS CREATE ───────────────────────────────────────────────
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


    # ─── ASYNCHRONOUS CREATE ───────────────────────────────────────────────
    async def create_async(self, model: TModel, defer_commit: bool = False) -> TModel:
        """
        Create a new model instance and add it to the database asynchronously.
        :param model: The model instance to create
        :param defer_commit: Whether to defer the commit of the transaction
        :return: The created model instance
        """
        self._db.add(model)
        await self._handle_defer_commit_single_model_async(model, defer_commit)
        return model


    # ─── SYNCHRONOUS UPDATE ───────────────────────────────────────────────
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


    # ─── ASYNCHRONOUS UPDATE ───────────────────────────────────────────────
    async def update_async(self, model: TModel, defer_commit: bool = False) -> TModel:
        """
        Update an existing model instance in the database asynchronously.
        :param model: The model instance to update
        :param defer_commit: Whether to defer the commit of the transaction
        :return: The updated model instance
        """
        if model.id is None:
            raise ValueError(
                f"The id of the existing model ({self.model.__name__}) is required for update action"
            )

        # Check if the model exists by attempting to fetch it asynchronously.
        if await self.exists_async({"id": model.id}):
            self._db.add(model)
            await self._handle_defer_commit_single_model_async(model, defer_commit)

            return model

        raise ValueError(
            f"Model of type {self.model.__name__} with id: {model.id} not found"
        )


    # ─── SYNCHRONOUS DELETE ───────────────────────────────────────────────
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


    # ─── ASYNCHRONOUS DELETE ────────────────────────────────────────────────
    async def delete_async(self, id: UUID, defer_commit: bool = False) -> None:
        """
        Asynchronously delete a single object as specified by its primary key.

        :param id: The model's primary key (e.g., 'id')
        :param defer_commit: Whether to defer the commit of the transaction.
        :return: None
        """
        model = await self._db.get(self.model, id)
        if model is not None:
            await self._db.delete(model)
        if not defer_commit:
            await self._db.commit()



    # ─── UTILITY METHODS ─────────────────────────────────────────────────────
    def _handle_defer_commit_single_model(self, model: TModel, defer_commit: bool) -> None:
        if defer_commit:
            self._db.flush()
        else:
            self._db.commit()
            self._db.refresh(model)

    async def _handle_defer_commit_single_model_async(self, model: TModel, defer_commit: bool) -> None:
        if defer_commit:
            await self._db.flush()
        else:
            await self._db.commit()
            await self._db.refresh(model)

    def _build_base_paginated_get_stmt(
            self,
            query_filter: Optional[Dict[str, Any]] = None,
            order_by: Optional[List[str]] = None,
            search_fields: Optional[List[str]] = None,
            search_text: Optional[str] = None,
    ):
        """
        Build the base SQLAlchemy Select statement with filters, search conditions,
        and ordering applied.
        """
        query_filter = query_filter or {}
        stmt = select(self.model)

        # Apply query filters.
        for key, value in query_filter.items():
            if hasattr(self.model, key):
                stmt = stmt.filter(getattr(self.model, key) == value)

        # Apply search filters.
        if search_text and search_fields:
            search_conditions = []
            joins_applied = set()
            for field_name in search_fields:
                # _get_column_and_joins() should return a tuple: (column, [join objects])
                column, joins = self._get_column_and_joins(self.model, field_name)
                # Ensure the column is a string/text type.
                if not isinstance(column.type, (String, Text)):
                    raise TypeError(f"Field '{field_name}' is not a string or text column")
                search_conditions.append(column.ilike(f"%{search_text}%"))
                # Apply necessary joins (only once per join)
                for join in joins:
                    if join not in joins_applied:
                        stmt = stmt.join(join)
                        joins_applied.add(join)
            if search_conditions:
                stmt = stmt.filter(or_(*search_conditions))

        # Apply ordering.
        if order_by:
            order_criteria = []
            joins_applied = set()
            for field in order_by:
                descending = False
                if field.startswith('-'):
                    descending = True
                    field = field[1:]
                column, joins = self._get_column_and_joins(self.model, field)
                for join in joins:
                    if join not in joins_applied:
                        stmt = stmt.join(join)
                        joins_applied.add(join)
                order_criteria.append(column.desc() if descending else column.asc())
            if order_criteria:
                stmt = stmt.order_by(*order_criteria)

        return stmt

    def _calculate_pagination(
            self,
            total: int,
            skip: Optional[int],
            limit: Optional[int]
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Given the total count, skip, and limit values, calculate the next and previous page numbers.
        """
        if limit is None:
            return (None, None)
        if skip is None:
            skip = 0
        next_page = ((skip + limit) // limit + 1) if (skip + limit < total) else None
        previous_page = max((skip - limit) // limit + 1, 1) if (skip and skip > 0) else None
        return (next_page, previous_page)

    def _get_column_and_joins(self, model, field_name):
        parts = field_name.split('.')
        current_model = model
        joins = []
        column = None
        for part in parts:
            if hasattr(current_model, part):
                attr = getattr(current_model, part)
                prop = getattr(attr, 'property', None)
                if isinstance(prop, RelationshipProperty):
                    # It's a relationship
                    current_model = prop.mapper.class_
                    joins.append(attr)
                else:
                    # It's a column
                    column = attr
            else:
                raise AttributeError(f"'{current_model.__name__}' has no attribute '{part}'")
        if column is None:
            raise AttributeError(f"Field '{field_name}' does not correspond to a valid column")
        return column, joins