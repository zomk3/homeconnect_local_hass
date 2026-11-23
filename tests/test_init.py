"""Tests for integration init."""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import ANY, AsyncMock, Mock

import pytest
from custom_components import homeconnect_ws
from custom_components.homeconnect_ws.const import INITIAL_CONNECT_DELAY, MAX_RECONECT_TIME
from homeassistant.config_entries import ConfigEntryState
from homeassistant.util import dt as dt_util
from homeconnect_websocket import ConnectionFailedError, ConnectionState
from homeconnect_websocket.testutils import MockAppliance
from pytest_homeassistant_custom_component.common import async_fire_time_changed

from .const import CONFIG_ENTRIES, DEVICE_DESCRIPTION

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant
    from pytest_homeassistant_custom_component.common import MockConfigEntry


@pytest.mark.parametrize(("config_entry"), CONFIG_ENTRIES)
async def test_load_unload_entry(
    config_entry: MockConfigEntry,
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test setup and unload config entry."""
    appliance = MockAppliance(DEVICE_DESCRIPTION, "host", "mock_app", "mock_app_id", "PSK_KEY")
    appliance_mock = Mock(return_value=appliance)
    monkeypatch.setattr(homeconnect_ws.coordinator, "HomeAppliance", appliance_mock)

    load_description_mock = Mock(return_value=DEVICE_DESCRIPTION)
    monkeypatch.setattr(homeconnect_ws, "load_description", load_description_mock)

    config_entry.add_to_hass(hass)
    await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done()

    assert config_entry.state is ConfigEntryState.LOADED

    appliance_mock.assert_called_once_with(
        description=DEVICE_DESCRIPTION,
        host="1.2.3.4",
        app_name="Homeassistant",
        app_id="Test_Device_ID",
        psk64="PSK_KEY",
        iv64="AES_IV",
        connection_callback=ANY,
    )

    assert await hass.config_entries.async_unload(config_entry.entry_id)
    await hass.async_block_till_done()

    assert config_entry.state is ConfigEntryState.NOT_LOADED

    appliance.session.close.assert_awaited_once()


@pytest.mark.parametrize(("config_entry"), CONFIG_ENTRIES[:1])
async def test_connect_retry_backoff(
    config_entry: MockConfigEntry,
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test failed connection attempts are retried with exponential backoff."""
    appliance = MockAppliance(DEVICE_DESCRIPTION, "host", "mock_app", "mock_app_id", "PSK_KEY")
    appliance.session.connect.side_effect = [
        ConnectionFailedError,
        TimeoutError,
        RuntimeError,
        None,
    ]
    appliance.session.connected = True
    monkeypatch.setattr(homeconnect_ws.coordinator, "HomeAppliance", Mock(return_value=appliance))
    monkeypatch.setattr(homeconnect_ws, "load_description", Mock(return_value=DEVICE_DESCRIPTION))
    sleep_mock = AsyncMock()
    monkeypatch.setattr(homeconnect_ws.coordinator, "asyncio", Mock(sleep=sleep_mock))

    config_entry.add_to_hass(hass)
    await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done(wait_background_tasks=True)

    assert appliance.session.connect.await_count == 4
    assert [call.args[0] for call in sleep_mock.await_args_list] == [
        INITIAL_CONNECT_DELAY,
        INITIAL_CONNECT_DELAY * 2,
        INITIAL_CONNECT_DELAY * 4,
    ]
    assert config_entry.runtime_data.coordinator.connected is True

    assert await hass.config_entries.async_unload(config_entry.entry_id)
    await hass.async_block_till_done()


@pytest.mark.parametrize(("config_entry"), CONFIG_ENTRIES[:1])
async def test_connection_state(
    config_entry: MockConfigEntry,
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test the Coordinator connection state follows the Session with a reconnect grace time."""
    appliance = MockAppliance(DEVICE_DESCRIPTION, "host", "mock_app", "mock_app_id", "PSK_KEY")
    appliance.session.connected = True
    monkeypatch.setattr(homeconnect_ws.coordinator, "HomeAppliance", Mock(return_value=appliance))
    monkeypatch.setattr(homeconnect_ws, "load_description", Mock(return_value=DEVICE_DESCRIPTION))

    config_entry.add_to_hass(hass)
    await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done(wait_background_tasks=True)
    coordinator = config_entry.runtime_data.coordinator
    assert coordinator.connected is True

    # Connection lost, reconnecting: still connected during the grace time
    appliance.session.connected = False
    await coordinator._connection_state_callback(ConnectionState.RECONNECTING)
    assert coordinator.connected is True

    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=MAX_RECONECT_TIME + 1))
    await hass.async_block_till_done()
    assert coordinator.connected is False

    # Reconnected
    appliance.session.connected = True
    await coordinator._connection_state_callback(ConnectionState.CONNECTED)
    assert coordinator.connected is True

    # Connection lost without reconnect
    appliance.session.connected = False
    await coordinator._connection_state_callback(ConnectionState.RECONNECTING)
    await coordinator._connection_state_callback(ConnectionState.ABNORMAL_CLOSURE)
    assert coordinator.connected is False

    assert await hass.config_entries.async_unload(config_entry.entry_id)
    await hass.async_block_till_done()


@pytest.mark.parametrize(("config_entry"), CONFIG_ENTRIES[:1])
async def test_reconnect_grace_not_restarted(
    config_entry: MockConfigEntry,
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test failed reconnect attempts don't start a new grace time."""
    appliance = MockAppliance(DEVICE_DESCRIPTION, "host", "mock_app", "mock_app_id", "PSK_KEY")
    appliance.session.connected = True
    monkeypatch.setattr(homeconnect_ws.coordinator, "HomeAppliance", Mock(return_value=appliance))
    monkeypatch.setattr(homeconnect_ws, "load_description", Mock(return_value=DEVICE_DESCRIPTION))

    config_entry.add_to_hass(hass)
    await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done(wait_background_tasks=True)
    coordinator = config_entry.runtime_data.coordinator

    appliance.session.connected = False
    await coordinator._connection_state_callback(ConnectionState.RECONNECTING)
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=MAX_RECONECT_TIME + 1))
    await hass.async_block_till_done()
    assert coordinator.connected is False

    # Reconnect attempt failed in the handshake, the library reports RECONNECTING again
    await coordinator._connection_state_callback(ConnectionState.HANDSHAKE)
    await coordinator._connection_state_callback(ConnectionState.RECONNECTING)
    assert coordinator.connected is False

    # A new connection loss after a successful reconnect gets a new grace time
    appliance.session.connected = True
    await coordinator._connection_state_callback(ConnectionState.CONNECTED)
    appliance.session.connected = False
    await coordinator._connection_state_callback(ConnectionState.RECONNECTING)
    assert coordinator.connected is True

    assert await hass.config_entries.async_unload(config_entry.entry_id)
    await hass.async_block_till_done()


@pytest.mark.parametrize(("config_entry"), CONFIG_ENTRIES[:1])
async def test_late_connected_event(
    config_entry: MockConfigEntry,
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test a CONNECTED event arriving after the connection was lost is ignored."""
    appliance = MockAppliance(DEVICE_DESCRIPTION, "host", "mock_app", "mock_app_id", "PSK_KEY")
    appliance.session.connected = True
    monkeypatch.setattr(homeconnect_ws.coordinator, "HomeAppliance", Mock(return_value=appliance))
    monkeypatch.setattr(homeconnect_ws, "load_description", Mock(return_value=DEVICE_DESCRIPTION))

    config_entry.add_to_hass(hass)
    await hass.config_entries.async_setup(config_entry.entry_id)
    await hass.async_block_till_done(wait_background_tasks=True)
    coordinator = config_entry.runtime_data.coordinator

    # The library delivers CONNECTED after the value fetch, the Session may be lost by then
    appliance.session.connected = False
    await coordinator._connection_state_callback(ConnectionState.RECONNECTING)
    await coordinator._connection_state_callback(ConnectionState.CONNECTED)
    assert coordinator.connected is True

    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=MAX_RECONECT_TIME + 1))
    await hass.async_block_till_done()
    assert coordinator.connected is False

    assert await hass.config_entries.async_unload(config_entry.entry_id)
    await hass.async_block_till_done()


@pytest.mark.parametrize(("config_entry"), CONFIG_ENTRIES)
async def test_remove_entry(
    config_entry: MockConfigEntry,
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test remove config entry."""
    unlink_mock = Mock()
    rmdir_mock = Mock()
    monkeypatch.setattr(Path, "unlink", unlink_mock)
    monkeypatch.setattr(Path, "rmdir", rmdir_mock)

    config_entry.add_to_hass(hass)

    assert await hass.config_entries.async_remove(config_entry.entry_id)
    if config_entry.version == 2:
        assert unlink_mock.call_count == 2
        rmdir_mock.assert_called_once()
    if config_entry.version == 1:
        unlink_mock.assert_not_called()
        rmdir_mock.assert_not_called()
