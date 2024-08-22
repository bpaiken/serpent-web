from enum import Enum


from sqlalchemy.types import TypeDecorator, String


class StringEnum(TypeDecorator):
    impl = String

    def __init__(self, enum_type, *args, **kwargs):
        assert issubclass(enum_type, Enum), "enum_type must be an Enum type"
        self.enum_type = enum_type
        super().__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        # If the value is an instance of the Enum, use its value
        if isinstance(value, Enum):
            value = value.value
        # Otherwise, ensure the value corresponds to one of the Enum's values
        elif value not in [e.value for e in self.enum_type]:
            raise ValueError(f"Invalid value: {value}")
        return value

    def process_result_value(self, value, dialect):
        # Convert the stored value back to an Enum member for Python code
        return self.enum_type(value)
