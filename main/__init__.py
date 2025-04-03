from importlib import import_module

from ._app import app
from ._config import config
from .commons.error_handlers import register_error_handlers


def register_subpackages():
    from main.controllers import router

    app.include_router(router)


register_subpackages()
register_error_handlers(app)
