"""State of a sensor & entity."""
import logging
from math import modf
from typing import Any, Callable, Optional, Union

import attr
from filter import Filter
from mqtt import (
    MQTT,
    Device,
    Entity,
    NumberEntity,
    SelectEntity,
    SensorEntity,
    hass_default_rw_icon,
    hass_device_class,
)
from options import OPT

from sunsynk.rwsensors import NumberRWSensor, RWSensor, SelectRWSensor, TimeRWSensor
from sunsynk.sunsynk import Sensor, Sunsynk

# sensor prefix per device id
SENSOR_PREFIX: dict[str, str] = {}
SS_TOPIC = "SUNSYNK/status"
_LOGGER = logging.getLogger(__name__)
SS = list(Sunsynk)


def tostr(val: Any) -> str:
    """Convert a value to a string with maximum 3 decimal places."""
    if val is None:
        return ""
    if not isinstance(val, float):
        return str(val)
    if modf(val)[0] == 0:
        return str(int(val))
    return f"{val:.3f}".rstrip("0")


@attr.define(slots=True)
class State:  # pylint: disable=too-few-public-methods
    """State of a sensor / entity."""

    filter: Optional[Filter] = attr.field()
    sensor: Sensor = attr.field()
    entity: Optional[Entity] = attr.field(default=None)
    "The entity will be None if hidden."
    hidden: bool = attr.field(default=False)
    "Hide state from HA."

    _last: Optional[Union[int, str]] = None
    retain: bool = False

    @property
    def value(self) -> Optional[Union[int, str]]:
        """Return the last value."""
        return self._last

    async def publish(self, val):
        """Set the value through MQTT."""
        if self.entity is None:
            _LOGGER.error("no entity %s", self.sensor.name)
            return
        if val is None or (self._last == val and self.retain):
            return
        await MQTT.connect(OPT)
        await MQTT.publish(
            topic=self.entity.state_topic,
            payload=tostr(val),
            retain=self.retain,
        )
        self._last = val

    @property
    def name(self) -> str:
        """Return the name of the sensor and filter."""
        nme = self.sensor.name if self.sensor else ""
        # nme = getattr(self.sensor, "name", str(self.sensor))
        # if isinstance(self, SCFilter):
        #     nme += ":step"
        return f"{nme}:{self.filter._filter.__name__}"  # pylint: disable=no-member,protected-access

    def create_entity(self, dev: Device) -> Entity:
        """Create HASS entities out of an existing list of filters"""

        def create_on_change_handler(state: State, value_func: Callable) -> Callable:
            def _handler(value: Any) -> None:
                SENSOR_WRITE_QUEUE[state.sensor] = value_func(value)

            return _handler

        if self.hidden:
            raise Exception("Do not create hidden entities!")

        sensor = self.sensor

        state_topic = f"{SS_TOPIC}/{OPT.sunsynk_id}/{sensor.id}"
        command_topic = f"{state_topic}_set"

        ent = {
            "device": dev,
            "name": f"{SENSOR_PREFIX[dev.id]} {sensor.name}".strip(),
            "state_topic": state_topic,
            "unique_id": f"{dev.id}_{sensor.id}",
            "unit_of_measurement": sensor.unit,
        }

        if isinstance(sensor, RWSensor):
            ent["entity_category"] = "config"
            ent["icon"] = hass_default_rw_icon(unit=sensor.unit)
        else:
            ent["device_class"] = hass_device_class(unit=sensor.unit)

        if isinstance(sensor, NumberRWSensor):
            self.entity = NumberEntity(
                **ent,
                command_topic=command_topic,
                min=float(sensor.min),  # todo: lookup!
                max=float(sensor.max),
                mode=OPT.number_entity_mode,
                on_change=create_on_change_handler(self, float),
                step=0.1 if sensor.factor < 1 else 1,
            )

        elif isinstance(sensor, SelectRWSensor):
            self.entity = SelectEntity(
                **ent,
                command_topic=command_topic,
                options=sensor.available_values(),
                on_change=create_on_change_handler(self, str),
            )

        elif isinstance(sensor, TimeRWSensor):
            ent["icon"] = "mdi:clock"
            self.entity = SelectEntity(
                **ent,
                command_topic=command_topic,
                options=sensor.available_values(15, SS[0].state.get),
                on_change=create_on_change_handler(self, str),
            )

        else:
            self.entity = SensorEntity(**ent)
        return self.entity


@attr.define(slots=True)
class TimeoutState(State):
    """Entity definition for the timeout sensor."""

    retain = True

    def create_entity(self, dev: Device) -> Entity:
        """MQTT entities for stats."""
        self.entity = SensorEntity(
            name=f"{SENSOR_PREFIX[dev.id]} RS485 timeouts",
            unique_id=f"{dev.id}_timeouts",
            state_topic=f"{SS_TOPIC}/{dev.id}/timeouts",
            entity_category="config",
            device=dev,
        )
        return self.entity


SENSOR_WRITE_QUEUE: dict[RWSensor, Union[str, int, float]] = {}
