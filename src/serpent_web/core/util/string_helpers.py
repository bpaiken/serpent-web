import re


def snake_to_pascal(string: str) -> str:
    """
    Example: "foo_bar" -> "FooBar"
    :param string:
    :return:
    """
    string_array = string.split('_')
    result = []
    for word in string_array:
        result.append(word.capitalize())
    return ''.join(result)


def snake_to_title(string: str) -> str:
    """
    Example: "foo_bar" -> "FooBar"
    :param string:
    :return:
    """
    return snake_to_pascal(string)


def snake_to_camel(string: str) -> str:
    """
    Example: "foo_bar" -> "fooBar"
    :param string:
    :return:
    """
    string_array = string.split('_')
    result = []
    for idx, word in enumerate(string_array):
        if idx == 0:
            result.append(word.lower())
        else:
            result.append(word.capitalize())

    return ''.join(result)


def title_to_snake(string: str) -> str:
    """
    Example: "FooBar" -> "foo_bar"
    :param string:
    :return:
    """
    return re.sub(r'(?<!^)(?=[A-Z])', '_', string).lower()
