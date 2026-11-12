"""Version two sales engine. Opt-in services; importing never touches production."""

from .store import Conflict, Scope, Store

__all__ = ['Conflict', 'Scope', 'Store']
