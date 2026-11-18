"""Private shared declaration/module text rule; contextual errors stay intact."""


def nonempty(value, location):
    if not isinstance(value, str) or not value.strip() or any(c in value for c in '\0\n\r'):
        raise ValueError(f'{location}: expected a nonempty string without NUL/newlines')
    return value
