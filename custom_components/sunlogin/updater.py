import asyncio
import logging
import uuid
from copy import deepcopy
from datetime import timedelta, datetime, timezone
from dataclasses import dataclass
from homeassistant.helpers.update_coordinator import (
    DataUpdateCoordinator,
    UpdateFailed
)
from homeassistant.const import CONF_DEVICES
from homeassistant.util import dt as dt_util
from homeassistant import config_entries
from .const import (
    CONF_DEVICE_NAME,
    CONF_DEVICE_MEMOS,
)

_LOGGER = logging.getLogger(__name__)

MIN_REMOTE_INTERVAL = 40
MIN_LOCAL_INTERVAL = 10

@dataclass
class UpdateInterval:
    _local: int = timedelta(seconds=MIN_LOCAL_INTERVAL)
    _remote: int = timedelta(seconds=MIN_REMOTE_INTERVAL)

    @property
    def local(self):
        return self._local
    
    @local.setter
    def local(self, value):
        if value <= MIN_LOCAL_INTERVAL:
            return
        self._local = timedelta(seconds=value)

    @property
    def remote(self):
        return self._remote
    
    @remote.setter
    def remote(self, value):
        if value <= MIN_REMOTE_INTERVAL:
            return
        self._remote = timedelta(seconds=value)

    def set(self, config):
        if (local := config.get('local')) is not None: 
            self.local = local
        if (remote := config.get('remote')) is not None:
            self.remote = remote


@dataclass
class PlugUpdateInterval:
    update_interval: UpdateInterval
    _interval: int

    @property
    def interval(self):
        if self._interval == 0:
            return self.update_interval.local
        elif self._interval == 1:
            return self.update_interval.remote
        else:
            return self.update_interval.remote
    
    @interval.setter
    def interval(self, value):
        self._interval = value
        
@dataclass
class GeneralUpdateInterval:
    _interval: timedelta

    def __init__(self, interval):
        self.interval = interval

    @property
    def interval(self):
        return self._interval
    
    @interval.setter
    def interval(self, value):
        self._interval = timedelta(seconds=value)


class StoreManager():
    def __init__(self, hass, entry):
        self.hass = hass
        self.entry = entry
        self.data = deepcopy(dict(entry.data))
        self.flag = False
        self.reload_flag = False

    def update_device_config(self, sn, data):
        self.data[CONF_DEVICES][sn].update(data)
        self.flag = True

    def update_device(self, devices):
        for sn, new_dev in devices.items():
            old_dev = self.data[CONF_DEVICES].get(sn)
            if old_dev is None:
                self.data[CONF_DEVICES][sn] = deepcopy(new_dev)
                self.flag = True
                self.reload_flag = True
                _LOGGER.debug(f"StoreManager({self.entry.entry_id}) new device added")
            # Cloud lists may omit optional metadata; omission is not a rename.
            elif any(
                key in new_dev and old_dev.get(key) != new_dev[key]
                for key in (CONF_DEVICE_NAME, CONF_DEVICE_MEMOS)
            ):
                self.data[CONF_DEVICES][sn].update(deepcopy(new_dev))
                self.flag = True
                self.reload_flag = True
                _LOGGER.debug(f"StoreManager({self.entry.entry_id}) device name updated")

    def update_token(self, data):
        self.data.update(data)
        self.flag = True

    def cancel(self):
        self.entry = None

    async def async_store_entry(self):
        entry = self.entry
        if entry is None:
            return
        if self.flag:
            self.hass.config_entries.async_update_entry(entry, data=deepcopy(self.data))
            self.flag = False
            _LOGGER.debug(f"StoreManager({entry.entry_id}) data write to entry")
        if self.reload_flag:
            self.reload_flag = False
            # HA may eagerly start the reload and cancel this manager immediately.
            _LOGGER.debug(f"StoreManager({entry.entry_id}) devices write to entry")
            self.hass.async_create_task(self.hass.config_entries.async_reload(entry.entry_id))
    


class UpdateManager():

    _tick = None

    def __init__(self, hass, entry):
        self.hass = hass
        self.entry = entry
        self.id = str(uuid.uuid4())
        self.tick = 10
        self.tasks = {}
        self.coordinator = DataUpdateCoordinator(
            hass,
            _LOGGER,
            name="SunloginUpdateScheduler",
            update_method=self.execute_tasks,
            update_interval=self.tick,
        )
        self.remove_listener = self.coordinator.async_add_listener(self.nop)
    
    @property
    def tick(self):
        return self._tick
    
    @tick.setter
    def tick(self, value):
        self._tick = timedelta(seconds=value)

    def nop(self):
        """"""

    async def execute_tasks(self):
        current_time = dt_util.utcnow()

        for task_name, task_info in list(self.tasks.items()):
            if self.entry is None:
                break
            if task_info is None:
                continue
            running = task_info.get('running')
            if running is not None and not running.done():
                continue
            if current_time >= task_info['next_run']:
                _LOGGER.debug(f"UpdateManager({self.entry.entry_id}) executing task {task_name}")
                task_info['running'] = self.entry.async_create_background_task(
                    self.hass, self._execute_task(task_name, task_info['task']), task_name
                )
                task_info['next_run'] = current_time + task_info['update_interval'].interval

    async def _execute_task(self, task_name, task):
        try:
            await task()
        except Exception:
            _LOGGER.exception("Sunlogin scheduled task %s failed", task_name)

    def add_task(self, task_name, task, update_interval, first_add=10):
        self.del_task(task_name)
        next_run = dt_util.utcnow() + timedelta(seconds=first_add)
        # if task_name in self.tasks:
        #     next_run = dt_util.utcnow() + update_interval.interval
        
        self.tasks[task_name] = {
            'task': task,
            'update_interval': update_interval,
            'next_run': next_run,
        }
        _LOGGER.debug(f"UpdateManager({self.entry.entry_id}) add_task {task_name} next_run: {next_run}")

    def del_task(self, task_name):
        task_info = self.tasks.pop(task_name, None)
        if task_info is not None:
            running = task_info.get('running')
            if running is not None and not running.done() and running is not asyncio.current_task():
                running.cancel()

    def clear_tasks(self):
        for task_name in list(self.tasks):
            self.del_task(task_name)
        _LOGGER.debug(f"UpdateManager({self.entry.entry_id}) clear_tasks")

    def cancel(self):
        self.clear_tasks()
        self.entry = None

DEFAULT_UPDATE_INTERVAL = UpdateInterval()
DEFAULT_POWER_CONSUMES_UPDATE_INTERVAL = GeneralUpdateInterval(1200)
DEFAULT_DNS_UPDATE_INTERVAL = GeneralUpdateInterval(360)
DEFAULT_CONFIG_UPDATE_INTERVAL = GeneralUpdateInterval(90)
DEFAULT_TOKEN_UPDATE_INTERVAL = GeneralUpdateInterval(300)
DEFAULT_DEVICES_UPDATE_INTERVAL = GeneralUpdateInterval(180)
