from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class BaseSchema(BaseModel):
    id: Optional[UUID] = Field(default=None)

    @field_validator("id")
    @classmethod
    def check_uuid4(cls, value: UUID):
        if value is not None and value.version != 4:
            raise ValueError("id must be a UUID version 4")
        return value

    @property
    def pk(self) -> UUID:
        return self.id

    class Config:
        orm_mode = True
