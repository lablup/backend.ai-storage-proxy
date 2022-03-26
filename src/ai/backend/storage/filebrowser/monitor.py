import asyncio
import time

from ai.backend.storage.context import Context

from .filebrowser import destroy_container, get_filebrowsers, get_network_stats


async def network_monitor(ctx: Context, container_id, freq, period):
    freq = ctx.local_config["filebrowser"]["freq"]
    period = ctx.local_config["filebrowser"]["period"]
    start_time = time.time()
    network_window = []
    while True:
        current_time = time.time()
        try:
            stats = await get_network_stats(container_id)
        except Exception:
            break
        network_total_transfer = stats[0] + stats[1]
        network_window.append(network_total_transfer)
        if current_time - start_time > period:
            network_utilization_change = network_window[-1] - network_window[0]
            if network_utilization_change == 0:
                start_time = current_time
                await destroy_container(ctx, container_id)
                break
            else:
                network_window = []
                start_time = current_time
        await asyncio.sleep(freq)


async def idle_timeout_monitor(ctx: Context, container_id, idle_timeout):
    start_time = time.time()
    while True:
        current_time = time.time()
        if current_time - start_time >= idle_timeout:
            await destroy_container(ctx, container_id)
            break
        await asyncio.sleep(1)


async def monitor(ctx: Context):
    idle_timeout = ctx.local_config["filebrowser"]["idle_timeout"]
    freq = ctx.local_config["filebrowser"]["freq"]
    period = ctx.local_config["filebrowser"]["period"]
    network_monitored_list = []
    idle_time_monitored_list = []
    while True:
        network_tasks = []
        idle_timeout_tasks = []
        browsers = await get_filebrowsers()
        if len(browsers) > 0:
            for browser in browsers:
                if browser not in network_monitored_list:
                    network_monitored_list.append(browser)
                    network_tasks.append(
                        asyncio.create_task(
                            network_monitor(ctx, browser, freq, period),
                        ),
                    )
                if (idle_timeout is not None) and (
                    browser not in idle_time_monitored_list
                ):
                    idle_time_monitored_list.append(browser)
                    idle_timeout_tasks.append(
                        asyncio.create_task(
                            idle_timeout_monitor(ctx, browser, idle_timeout),
                        ),
                    )
        if len(network_tasks) > 0:
            await asyncio.gather(*network_tasks)
        if len(idle_timeout_tasks) > 0:
            await asyncio.gather(*idle_timeout_tasks)
        await asyncio.sleep(10)
