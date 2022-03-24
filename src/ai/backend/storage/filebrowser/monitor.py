import asyncio

from ai.backend.storage.context import Context
from .filebrowser import (
    destroy_container,
    get_filebrowsers,
    get_network_stats,
)


async def network_monitor(ctx: Context, container_id, freq, period, threshold):

    network_window = []

    while True:
        stats = await get_network_stats(container_id)
        network_total_transfer = stats[0] + stats[1]
        network_window.append(network_total_transfer)
        if len(network_window) > period:
            network_utilization_change = network_window[-1] - network_window[0]
            if network_utilization_change == 0:
                await destroy_container(ctx, container_id)
                break
        await asyncio.sleep(freq)
    return 1


async def monitor(ctx: Context):

    monitored_list = []
    while True:
        tasks = []
        browsers = await get_filebrowsers()
        if len(browsers) > 0:
            for browser in browsers:
                if browser not in monitored_list:
                    monitored_list.append(browser)
                    tasks.append(asyncio.create_task(network_monitor(ctx, browser, 1, 10, 1024)))

        await asyncio.sleep(10)
        await asyncio.gather(*tasks)
