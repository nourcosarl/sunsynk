#!/usr/bin/env python3
"""Run the addon."""
import asyncio
import logging
import sys
import traceback
from asyncio.events import AbstractEventLoop
from collections import defaultdict
from typing import Sequence

from filter import RROBIN, Filter, getfilter, suggested_filter
from mqtt import MQTT, Device
from options import OPT, init_options
from state import SENSOR_PREFIX, SENSOR_WRITE_QUEUE, SS_TOPIC, State, TimeoutState

from sunsynk.definitions import (
    ALL_SENSORS,
    DEPRECATED,
    RATED_POWER,
    SERIAL,
    WATT,
    MathSensor,
)
from sunsynk.helpers import slug
from sunsynk.rwsensors import RWSensor
from sunsynk.sunsynk import Sensor, Sunsynk

SUNSYNK: Sunsynk = None  # type: ignore

_LOGGER = logging.getLogger(__name__)


HASS_DISCOVERY_INFO_UPDATE_QUEUE: set[str] = set()
# SERIAL = ALL_SENSORS["serial"]
STARTUP_SENSORS: list[Sensor] = []
STATES: dict[str, State] = {}


async def publish_sensors(states: list[State], *, force: bool = False) -> None:
    """Publish state to HASS."""
    for state in states:
        if state.hidden or state.sensor is None:
            continue
        val = SUNSYNK.state[state.sensor]
        if isinstance(state.filter, Filter):
            val = state.filter.update(val)
            if force and val is None:
                val = SUNSYNK.state[state.sensor]
        await state.publish(val)

    # statistics
    await STATES["to"].publish(SUNSYNK.timeouts)


async def hass_discover_sensors(serial: str, rated_power: float) -> None:
    """Discover all sensors."""
    dev = Device(
        identifiers=[OPT.sunsynk_id],
        name=f"Sunsynk Inverter {serial}",
        model=f"{int(rated_power/1000)}kW Inverter {serial}",
        manufacturer="Sunsynk",
    )
    SENSOR_PREFIX[OPT.sunsynk_id] = OPT.sensor_prefix
    ents = [s.create_entity(dev) for s in STATES.values() if not s.hidden]
    await MQTT.connect(OPT)
    await MQTT.publish_discovery_info(entities=ents)


async def hass_update_discovery_info() -> None:
    """Update discovery info for existing sensors"""
    states = [STATES[n] for n in HASS_DISCOVERY_INFO_UPDATE_QUEUE]
    HASS_DISCOVERY_INFO_UPDATE_QUEUE.clear()
    ents = [s.create_entity(s.entity.device) for s in states if not s.hidden]
    await MQTT.connect(OPT)
    await MQTT.publish_discovery_info(entities=ents, remove_entities=False)


def enqueue_hass_discovery_info_update(sen: Sensor, deps: list[Sensor]) -> None:
    """Add a sensor's dependants to the HASS discovery info update queue."""
    ids = sorted(s.id for s in deps)
    _LOGGER.debug(
        "%s changed: Enqueue discovery info updates for %s", sen.name, ", ".join(ids)
    )
    HASS_DISCOVERY_INFO_UPDATE_QUEUE.update(ids)


def setup_driver() -> None:
    """Setup the correct driver."""
    # pylint: disable=import-outside-toplevel
    global SUNSYNK  # pylint: disable=global-statement
    if OPT.driver == "pymodbus":
        from sunsynk.pysunsynk import pySunsynk

        SUNSYNK = pySunsynk()
        if not OPT.port:
            OPT.port = OPT.device

    elif OPT.driver == "umodbus":
        from sunsynk.usunsynk import uSunsynk

        SUNSYNK = uSunsynk()
        if not OPT.port:
            OPT.port = "serial://" + OPT.device

    else:
        _LOGGER.critical("Invalid DRIVER: %s. Expected umodbus, pymodbus", OPT.driver)
        sys.exit(-1)

    SUNSYNK.port = OPT.port
    SUNSYNK.server_id = OPT.modbus_server_id
    SUNSYNK.timeout = OPT.timeout
    SUNSYNK.read_sensors_batch_size = OPT.read_sensors_batch_size
    STATES["to"] = TimeoutState(entity=None, filter=None, sensor=None)


def setup_sensors() -> None:
    """Setup the sensors."""
    # pylint: disable=too-many-branches
    sens: dict[str, Filter] = {}
    sens_dependants: dict[str, list[Sensor]] = defaultdict(list)
    startup_sens = {SERIAL.id, RATED_POWER.id}

    msg: dict[str, list[str]] = defaultdict(list)

    # Add test sensors
    ALL_SENSORS.update((s.id, s) for s in TEST_SENSORS)

    for sensor_def in OPT.sensors:
        name, _, fstr = sensor_def.partition(":")
        name = slug(name)
        if name in sens:
            _LOGGER.warning("Sensor %s only allowed once", name)
            continue

        sen = ALL_SENSORS.get(name)
        if not isinstance(sen, Sensor):
            log_bold(f"Unknown sensor in config: {sensor_def}")
            continue
        if sen.id in DEPRECATED:
            log_bold(f"Sensor deprecated: {sen.id} -> {DEPRECATED[sen.id].id}")
        if not fstr:
            fstr = suggested_filter(sen)
            msg[f"*{fstr}"].append(name)  # type: ignore
        else:
            msg[fstr].append(name)  # type: ignore

        filt = getfilter(fstr, sensor=sen)
        STATES[sen.id] = State(
            sensor=sen, filter=filt, retain=isinstance(sen, RWSensor)
        )
        sens[sen.id] = filt

        if isinstance(sen, RWSensor):
            for dep in sen.dependencies:
                sens_dependants[dep.id].append(sen)

    for sen_id, deps in sens_dependants.items():
        if sen_id not in ALL_SENSORS:
            _LOGGER.fatal("Invalid sensor as dependency - %s", sen_id)
        sen = ALL_SENSORS[sen_id]

        if sen_id not in sens and sen != RATED_POWER:  # Rated power does not change
            fstr = suggested_filter(sen)
            msg[f"*{fstr}"].append(name)  # type: ignore
            filt = getfilter(fstr, sensor=sen)
            STATES[sen.id] = State(
                sensor=sen, filter=filt, retain=isinstance(sen, RWSensor), hidden=True
            )
            sens[sen.id] = filt
            _LOGGER.info("Added hidden sensor %s as other sensors depend on it", sen_id)

        startup_sens.add(sen_id)
        sen.on_change = lambda s=sen, d=deps: enqueue_hass_discovery_info_update(s, d)

    # Add any sensor dependencies to STARTUP_SENSORS
    STARTUP_SENSORS.clear()
    STARTUP_SENSORS.extend(ALL_SENSORS[n] for n in startup_sens)
    for nme, val in msg.items():
        _LOGGER.info("Filter %s used for %s", nme, ", ".join(sorted(val)))


def log_bold(msg: str) -> None:
    """Log a message."""
    _LOGGER.info("#" * 60)
    _LOGGER.info(f"{msg:^60}".rstrip())
    _LOGGER.info("#" * 60)


READ_ERRORS = 0


async def read_sensors(
    sensors: Sequence[Sensor], msg: str = "", retry_single: bool = False
) -> bool:
    """Read from the Modbus interface."""
    global READ_ERRORS  # pylint:disable=global-statement
    try:
        await SUNSYNK.read_sensors(sensors)
        READ_ERRORS = 0
        return True
    except asyncio.TimeoutError:
        pass
    except Exception as err:  # pylint:disable=broad-except
        _LOGGER.error("Read Error%s: %s", msg, err)
        if OPT.debug > 2:
            traceback.print_exc()
        READ_ERRORS += 1
        if READ_ERRORS > 3:
            raise Exception(f"Multiple Modbus read errors: {err}") from err

    if retry_single:
        _LOGGER.info("Retrying individual sensors: %s", [s.id for s in sensors])
        for sen in sensors:
            await asyncio.sleep(0.02)
            await read_sensors([sen], msg=sen.name, retry_single=False)

    return False


TERM = (
    "This Add-On will terminate in 30 seconds, "
    "use the Supervisor Watchdog to restart automatically."
)


async def main(loop: AbstractEventLoop) -> None:  # noqa
    """Main async loop."""
    # pylint: disable=too-many-statements
    loop.set_debug(OPT.debug > 0)

    try:
        await SUNSYNK.connect()
    except ConnectionError:
        log_bold(f"Could not connect to {SUNSYNK.port}")
        _LOGGER.critical(TERM)
        await asyncio.sleep(30)
        return

    _LOGGER.info(
        "Reading startup sensors %s", ", ".join([s.id for s in STARTUP_SENSORS])
    )

    if not await read_sensors(STARTUP_SENSORS):
        log_bold(
            f"No response on the Modbus interface {SUNSYNK.port}, try checking the "
            "wiring to the Inverter, the USB-to-RS485 converter, etc"
        )
        _LOGGER.critical(TERM)
        await asyncio.sleep(30)
        return

    log_bold(f"Inverter serial number '{SUNSYNK.state[SERIAL]}'")

    if OPT.sunsynk_id != SUNSYNK.state[SERIAL] and not OPT.sunsynk_id.startswith("_"):
        log_bold("SUNSYNK_ID should be set to the serial number of your Inverter!")
        return

    powr = float(5000)
    try:
        powr = float(SUNSYNK.state[RATED_POWER])  # type:ignore
    except (ValueError, TypeError):
        pass

    # Start MQTT
    MQTT.availability_topic = f"{SS_TOPIC}/{OPT.sunsynk_id}/availability"
    await hass_discover_sensors(str(SUNSYNK.state[SERIAL]), powr)

    # Read all & publish immediately
    await asyncio.sleep(0.01)
    await read_sensors(
        [s.sensor for s in STATES.values() if s.sensor], retry_single=True
    )
    await publish_sensors(STATES.values(), force=True)

    async def write_sensors() -> None:
        """Flush any pending sensor writes"""

        while SENSOR_WRITE_QUEUE:
            sensor, value = SENSOR_WRITE_QUEUE.popitem()
            await SUNSYNK.write_sensor(sensor, value)  # , msg=f"[old {old_reg_value}]")
            await read_sensors([sensor], msg=sensor.name)
            await publish_sensors([STATES[sensor.id]], force=True)

    async def poll_sensors() -> None:
        """Poll sensors."""
        states: list[State] = []
        # 1. collect sensors to read
        RROBIN.tick()
        for state in STATES.values():
            if state.filter and state.filter.should_update():
                states.append(state)
        if states:
            # 2. read
            if await read_sensors([s.sensor for s in states]):
                # 3. decode & publish
                await publish_sensors(states)

    while True:
        await write_sensors()

        polltask = asyncio.create_task(poll_sensors())
        await asyncio.sleep(1)
        try:
            await polltask
        except asyncio.TimeoutError as exc:
            _LOGGER.error("TimeOut %s", exc)
            continue
        except AttributeError as exc:
            _LOGGER.error("Attr err %s", exc)
            # The read failed. Exit and let the watchdog restart
            return
        if HASS_DISCOVERY_INFO_UPDATE_QUEUE:
            await hass_update_discovery_info()


TEST_SENSORS = (
    MathSensor(
        (175, 167, 166), "Essential abs power", WATT, factors=(1, 1, -1), absolute=True
    ),
    # https://powerforum.co.za/topic/8646-my-sunsynk-8kw-data-collection-setup/?do=findComment&comment=147591
    MathSensor(
        (175, 169, 166), "Essential l2 power", WATT, factors=(1, 1, -1), absolute=True
    ),
)


if __name__ == "__main__":
    init_options()
    setup_driver()
    setup_sensors()
    LOOP = asyncio.get_event_loop()
    LOOP.run_until_complete(main(LOOP))
    LOOP.close()
