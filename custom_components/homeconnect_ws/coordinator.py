"""Home Connect Coordinator."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from homeassistant.const import CONF_DEVICE_ID, CONF_HOST
from homeassistant.core import CALLBACK_TYPE, callback
from homeassistant.exceptions import ConfigEntryError
from homeassistant.helpers.event import async_call_later
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeconnect_websocket import (
    AllreadyConnectedError,
    ConnectionFailedError,
    ConnectionState,
    DeviceDescription,
    HCHandshakeError,
    HomeAppliance,
)

from .const import (
    CONF_AES_IV,
    CONF_PSK,
    INITIAL_CONNECT_DELAY,
    MAX_CONNECT_DELAY,
    MAX_RECONECT_TIME,
)

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

    from . import HCConfigEntry

_LOGGER = logging.getLogger(__name__)


class HomeConnectCoordinator(DataUpdateCoordinator):
    """My custom coordinator."""

    config_entry: HCConfigEntry
    appliance: HomeAppliance
    _connecting: bool = True
    _reconnecting: bool = False
    _reconnect_grace_cancel: CALLBACK_TYPE | None = None

    def __init__(
        self, hass: HomeAssistant, config_entry: HCConfigEntry, description: DeviceDescription
    ) -> None:
        """Initialize the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            # Name of the data. For logging purposes.
            name=description["info"]["model"],
            config_entry=config_entry,
            always_update=True,
        )

        self.appliance = HomeAppliance(
            description=description,
            host=config_entry.data[CONF_HOST],
            app_name="Homeassistant",
            app_id=config_entry.data[CONF_DEVICE_ID],
            psk64=config_entry.data[CONF_PSK],
            iv64=config_entry.data.get(CONF_AES_IV, None),
            connection_callback=self._connection_state_callback,
        )
        if not self.appliance.info:
            msg = "Appliance has no device info"
            raise ConfigEntryError(msg)

    @property
    def connected(self) -> bool:
        """
        Return if the Appliance is considered connected.

        The Session state is the source of truth. While the Session is reconnecting,
        the Appliance stays connected for up to MAX_RECONECT_TIME to hide short dropouts.
        """
        return self.appliance.session.connected or self._reconnect_grace_cancel is not None

    async def close(self) -> None:
        self._connecting = False
        self._end_reconnect_grace()
        await self.appliance.close()

    async def _async_setup(self) -> None:
        self.config_entry.async_create_background_task(
            self.hass, self._connect(), f"homeconnect_ws_{self.name}"
        )

    async def _connect(self) -> None:
        self.logger.debug("Connecting to %s", self.appliance.info.get("vib"))
        host = self.config_entry.data[CONF_HOST]
        first_failure = True
        backoff = INITIAL_CONNECT_DELAY
        while self._connecting:
            try:
                await self.appliance.connect()
                if self.appliance.session.connected:
                    if not first_failure:
                        self.logger.info("Connected to %s", host)
                    # The library reports CONNECTED to the connection callback only after
                    # the initial value fetch, notify listeners as soon as the Session is up
                    self.async_set_updated_data(None)
                    return
            except (ConnectionFailedError, HCHandshakeError, TimeoutError):
                await self.appliance.close()
                if first_failure:
                    self.logger.error("Can't connect to %s, retrying", host)  # noqa: TRY400
                    first_failure = False
                else:
                    self.logger.debug("Can't connect to %s, retrying", host)
            except AllreadyConnectedError:
                await self.appliance.close()
                self.logger.error("Allready connected to %s", host)  # noqa: TRY400
                return
            except Exception:
                await self.appliance.close()
                self.logger.exception("Can't connect to %s, retrying", host)
                first_failure = False

            # Wait before retrying, an unreachable appliance often fails immediately
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, MAX_CONNECT_DELAY)

    async def _async_update_data(self) -> None:
        return None

    async def _connection_state_callback(self, event: ConnectionState) -> None:
        if event == ConnectionState.RECONNECTING:
            # Reported again after each failed attempt, only a new connection loss
            # starts the grace time
            if not self._reconnecting:
                self._reconnecting = True
                self._reconnect_grace_cancel = async_call_later(
                    self.hass, MAX_RECONECT_TIME, self._reconnect_grace_expired
                )
        elif event == ConnectionState.CONNECTED:
            # Reported after the initial value fetch, the Session may be lost again by then
            if self.appliance.session.connected:
                if self._reconnecting:
                    self.logger.debug("Reconnected to %s", self.appliance.info.get("vib"))
                self._reconnecting = False
                self._end_reconnect_grace()
        elif event in (ConnectionState.CLOSED, ConnectionState.ABNORMAL_CLOSURE):
            self._reconnecting = False
            self._end_reconnect_grace()

        self.async_set_updated_data(None)

    def _end_reconnect_grace(self) -> None:
        if self._reconnect_grace_cancel is not None:
            self._reconnect_grace_cancel()
            self._reconnect_grace_cancel = None

    @callback
    def _reconnect_grace_expired(self, _: object) -> None:
        self._reconnect_grace_cancel = None
        self.async_set_updated_data(None)
