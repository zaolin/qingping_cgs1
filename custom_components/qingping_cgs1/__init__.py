"""The Qingping Device integration."""
from __future__ import annotations

from typing import Callable
import logging

from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import (
    DOMAIN,
    CONF_TEMPERATURE_OFFSET,
    CONF_HUMIDITY_OFFSET,
    DEFAULT_OFFSET,
    CONF_UPDATE_INTERVAL,
    DEFAULT_UPDATE_INTERVAL,
    MQTT_TOPIC_PREFIX,
)

PLATFORMS: list[Platform] = [
    Platform.SENSOR,
    Platform.NUMBER,
    Platform.SELECT,
    Platform.SWITCH,
    Platform.BUTTON,
    Platform.TIME,
]

_LOGGER = logging.getLogger(__name__)

_MQTT_HANDLERS: dict[str, list[Callable]] = {}
_MQTT_TOPIC_BASE = f"{MQTT_TOPIC_PREFIX}/+/up"


@callback
def _dispatch_message(message) -> None:
    """Dispatch a received MQTT message to the handler(s) registered for its MAC."""
    try:
        topic = str(message.topic)
        mac = topic.split("/")[1].upper()
    except (IndexError, AttributeError):
        return
    for handler in list(_MQTT_HANDLERS.get(mac, [])):
        try:
            handler(message)
        except Exception as err:  # noqa: BLE001
            _LOGGER.error("Handler error for MAC %s: %s", mac, err)


async def async_subscribe_for_mac(hass: HomeAssistant, mac: str, handler: Callable) -> None:
    """Register a per-MAC message handler. Lazily subscribes once per MAC."""
    mac = mac.upper()
    handlers = _MQTT_HANDLERS.setdefault(mac, [])
    if not handlers:
        _LOGGER.info("Subscribing to MQTT topic for MAC %s", mac)
        await mqtt.async_subscribe(
            hass, _MQTT_TOPIC_BASE, _dispatch_message, 1, encoding=None
        )
    if handler not in handlers:
        handlers.append(handler)


@callback
def async_unsubscribe_for_mac(mac: str, handler: Callable) -> None:
    """Remove a previously registered handler for a MAC."""
    mac = mac.upper()
    handlers = _MQTT_HANDLERS.get(mac, [])
    if handler in handlers:
        handlers.remove(handler)
    if not handlers:
        _MQTT_HANDLERS.pop(mac, None)


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Qingping Device from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    async def async_update_data():
        """Fetch data from API endpoint."""
        return {}

    coordinator = DataUpdateCoordinator(
        hass,
        _LOGGER,
        name="sensor",
        update_method=async_update_data,
        update_interval=None,
    )

    await coordinator.async_config_entry_first_refresh()

    hass.data[DOMAIN][entry.entry_id] = {
        "config": entry.data,
        CONF_TEMPERATURE_OFFSET: entry.data.get(CONF_TEMPERATURE_OFFSET, DEFAULT_OFFSET),
        CONF_HUMIDITY_OFFSET: entry.data.get(CONF_HUMIDITY_OFFSET, DEFAULT_OFFSET),
        CONF_UPDATE_INTERVAL: entry.data.get(CONF_UPDATE_INTERVAL, DEFAULT_UPDATE_INTERVAL),
        "coordinator": coordinator,
    }

    coordinator.data = hass.data[DOMAIN][entry.entry_id]

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload a config entry."""
    # Cancel any in-flight debounced setting-change tasks to avoid leaks
    try:
        from .sensor import _pending_setting_publishes
        mac = (entry.data.get("mac") or "").upper()
        for key in [k for k in list(_pending_setting_publishes.keys()) if k.startswith(f"{mac}_")]:
            _pending_setting_publishes.pop(key, None)
    except Exception:
        pass

    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id, None)
    return True
