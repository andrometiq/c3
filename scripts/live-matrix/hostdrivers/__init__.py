"""Pure registration. Construction never probes or launches a host."""
from hostdriver import DriverError


class Registry:
    def __init__(self):
        self.factories = {}

    def register(self, driver_id, factory):
        if type(driver_id) is not str or not driver_id or driver_id in self.factories or not callable(factory):
            raise DriverError('register', 'registry', 'invalid_request', 'invalid or duplicate driver registration')
        self.factories[driver_id] = factory

    def create(self, driver_id, **configuration):
        if driver_id not in self.factories:
            raise DriverError('create', 'registry', 'unsupported', 'driver is not registered')
        return self.factories[driver_id](**configuration)


def registry():
    from hostdrivers.claude import ClaudeHostDriver
    result = Registry()
    result.register('claude', ClaudeHostDriver)
    return result
