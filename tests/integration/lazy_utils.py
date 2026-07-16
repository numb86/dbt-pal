# This module must be imported only inside python_model_function_import.py's model() function.
# If any other model imported it at top level, Python would cache it in sys.modules and
# the function-body import would succeed regardless of sys.path, masking the bug this tests.
def add_twenty(value):
    return value + 20
