import asyncio

"""
from ..filebrowser.filebrowser import (
    destroy_container,
    get_container_by_id,
    get_filebrowsers,
    get_network_stats,
)

from .filebrowser import (
    destroy_container,
    get_container_by_id,
    get_filebrowsers,
    get_network_stats,
)
"""

from filebrowser import destroy_container

async def network_monitor(container_id, freq, period, threshold):

    network_window = []
    container = get_container_by_id(container_id)

    while True:
        stats = await get_network_stats(container)
        network_total_transfer = stats[0] + stats[1]
        network_window.append(network_total_transfer)
        print("network total: ", network_total_transfer)

        if len(network_window) > period:
            network_utilization_change = network_window[-1] - network_window[0]
            if network_utilization_change == 0:
                await destroy_container(container_id)
                break

        asyncio.sleep(freq)
    return 1


async def main():
    monitored_list = []
    while True:
        browsers = await get_filebrowsers()
        if len(browsers) > 0:
            for browser in browsers:
                if browser not in monitored_list:
                    monitored_list.append(browser)
                    asyncio.create_task(network_monitor(browser, 1, 10, 1024))

        asyncio.sleep(10)


if __name__ == "__main__":
    print("Start")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
