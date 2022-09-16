import requests

from . import monad

class CliError(Exception):
    """
    Base Error Class for Job errors
    """

    def __init__(self, message="", name="", ctx={}, code=500, klass="", retryable=False, traceback: str = None):
        self.code = 500 if code is None else code
        self.retryable = retryable
        self.message = message
        self.name = name
        self.ctx = ctx
        self.klass = klass
        self.traceback = traceback
        super().__init__(self.message)

    def error(self):
        return {'error': self.message, 'code': self.code, 'step': self.name, 'ctx': self.ctx}

    def __str__(self):
        return f"error: {self.message}, code: {self.code}"


def http_error_test_fn(result) -> monad.EitherMonad:
    if result.is_left():
        return result
    if result.is_right() and result.value.status_code in [200, 201]:
        return result
    return monad.Left(generate_http_error(result.value))


def generate_http_error(resp: requests.models.Response) -> CliError:
    return CliError(message=f"Http Error when calling {resp.url} with statuscode: {resp.status_code}",
                    code=resp.status_code,
                    name=__name__,
                    ctx={'contentType': content_type(resp),
                         'content': content_parser(resp)}
                    )

def content_parser(resp):
    ctype = content_type(resp)
    if "json" in ctype:
        return resp.json()
    return resp.text


def content_type(resp):
    ctype = resp.headers.get('Content-Type', None)
    if not ctype:
        return None
    parts = ctype.split(";")
    if not parts:
        return None
    return parts[0]