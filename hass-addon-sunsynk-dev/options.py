"""Addon options."""
import logging
from json import loads
from pathlib import Path

import attr
import yaml

from sunsynk.helpers import slug

_LOGGER = logging.getLogger(__name__)


@attr.define(slots=True)
class Options:
    """HASS Addon Options."""

    # pylint: disable=too-few-public-methods
    mqtt_host: str = ""
    mqtt_port: int = 0
    mqtt_username: str = ""
    mqtt_password: str = ""
    number_entity_mode: str = "auto"
    sunsynk_id: str = ""
    sensors: list[str] = []
    read_sensors_batch_size: int = 60
    profiles: list[str] = []
    sensor_prefix: str = ""
    timeout: int = 10
    debug: int = 1
    port: str = ""
    device: str = ""
    modbus_server_id: int = 1
    driver: str = "umodbus"

    def update(self, json: dict) -> None:
        """Update options."""
        for key, val in json.items():
            setattr(self, key.lower(), val)
        self.sunsynk_id = slug(self.sunsynk_id)


OPT = Options()


def init_options():
    """Initialize the options & logger."""
    logging.basicConfig(
        format="%(asctime)s %(levelname)-7s %(message)s", level=logging.DEBUG
    )

    hassosf = Path("/data/options.json")
    if hassosf.exists():
        _LOGGER.info("Loading HASS OS configuration")
        OPT.update(loads(hassosf.read_text(encoding="utf-8")))
    else:
        _LOGGER.info(
            "Local test mode - Defaults apply. Pass MQTT host & password as arguments"
        )
        configf = Path(__file__).parent / "config.yaml"
        OPT.update(yaml.safe_load(configf.read_text()).get("options", {}))
        localf = Path(__file__).parent.parent / ".local.yaml"
        if localf.exists():
            OPT.update(yaml.safe_load(localf.read_text()))

    if OPT.profiles:
        _LOGGER.info("PROFILES were deprecated. Please remove this configuration.")

    if OPT.debug < 2:
        logging.basicConfig(
            format="%(asctime)s %(levelname)-7s %(message)s",
            level=logging.INFO,
            force=True,
        )
