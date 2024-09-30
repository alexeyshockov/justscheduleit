# This is the same as using the @pytest.mark.anyio on all test functions in the module

import pytest
from anyio import fail_after
from anyio.from_thread import BlockingPortal
from pytest_mock import MockerFixture

from justscheduleit.hosting import SyncService, ServiceLifetime

pytestmark = pytest.mark.anyio


@pytest.fixture
async def portal():
    async with BlockingPortal() as portal:
        yield portal


async def test_sync_service_param_detection():
    def simple_sync_service():
        pass

    s = SyncService(simple_sync_service)
    assert s.lifetime_aware == False

    def sync_service1(lifetime: ServiceLifetime):
        pass

    s = SyncService(sync_service1)
    assert s.lifetime_aware == False

    def sync_service2(service_lifetime: ServiceLifetime):
        pass

    s = SyncService(sync_service2)
    assert s.lifetime_aware == True


async def test_sync_service(portal, mocker: MockerFixture):
    sync_service = mocker.MagicMock()
    service_lifetime = mocker.MagicMock()
    service_lifetime.host_portal = portal
    service_lifetime.set_started = mocker.MagicMock()

    s = SyncService(sync_service)
    s.lifetime_aware = False

    with fail_after(1):
        await s.execute(service_lifetime)

    sync_service.assert_called_once()

    service_lifetime.set_started.assert_called_once()


async def test_lifetime_aware_sync_service(portal, mocker: MockerFixture):
    sync_service = mocker.MagicMock()
    service_lifetime = mocker.MagicMock(ServiceLifetime)
    service_lifetime.host_portal = portal
    service_lifetime.set_started = mocker.MagicMock()

    s = SyncService(sync_service)
    s.lifetime_aware = True

    with fail_after(1):
        await s.execute(service_lifetime)

    service_lifetime.set_started.assert_not_called()

    sync_service.assert_called_once_with(service_lifetime)
