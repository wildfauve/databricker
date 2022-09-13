from . import monad

def http_error_test_fn(result) -> monad.EitherMonad:
    if result.is_left():
        return result
    if result.is_right() and result.value.status_code in [200, 201]:
        return result
    return monad.Left(result.value)

