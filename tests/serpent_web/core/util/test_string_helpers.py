import pytest
from serpent_web.core.util.string_helpers import snake_to_pascal, snake_to_title, snake_to_camel, title_to_snake

@pytest.mark.unit
def test_snake_to_pascal():
    assert snake_to_pascal("foo_bar") == "FooBar"
    assert snake_to_pascal("hello_world") == "HelloWorld"
    assert snake_to_pascal("snake_case_string") == "SnakeCaseString"
    assert snake_to_pascal("") == ""

@pytest.mark.unit
def test_snake_to_title():
    assert snake_to_title("foo_bar") == "FooBar"
    assert snake_to_title("hello_world") == "HelloWorld"
    assert snake_to_title("snake_case_string") == "SnakeCaseString"
    assert snake_to_title("") == ""

@pytest.mark.unit
def test_snake_to_camel():
    assert snake_to_camel("foo_bar") == "fooBar"
    assert snake_to_camel("hello_world") == "helloWorld"
    assert snake_to_camel("snake_case_string") == "snakeCaseString"
    assert snake_to_camel("") == ""

@pytest.mark.unit
def test_title_to_snake():
    assert title_to_snake("FooBar") == "foo_bar"
    assert title_to_snake("HelloWorld") == "hello_world"
    assert title_to_snake("SnakeCaseString") == "snake_case_string"
    assert title_to_snake("") == ""