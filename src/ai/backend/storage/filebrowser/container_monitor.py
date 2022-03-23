import asyncio

import aiodocker


async def get_filebrowsers():

    docker = aiodocker.Docker()
    container_list = []

    containers = await aiodocker.docker.DockerContainers(docker).list()

    for container in containers:
        stats = await container.stats(stream=False)
        print(container._id, stats)
        name = stats[0]["name"]
        cnt_id = stats[0]["id"]

        if "FileBrowser" in name:
            container_list.append(cnt_id)

    await docker.close()
    return container_list


async def destroy_container(container_id):
    docker = aiodocker.Docker()
    container = aiodocker.docker.DockerContainer(docker, id=container_id)
    await container.stop()
    await container.delete()
    await docker.close()
    print("done")
    return 1


async def network_monitor(container_id, freq, period, threshold):

    network_window = []
    docker = aiodocker.Docker()
    container = aiodocker.docker.DockerContainer(docker, id=container_id)

    while True:
        stats = await container.stats(stream=False)
        network_total_transfer = (
            stats[0]["networks"]["eth0"]["rx_bytes"]
            + stats[0]["networks"]["eth0"]["tx_bytes"]
        )
        network_window.append(network_total_transfer)
        print("network total: ", network_total_transfer)

        if len(network_window) > period:
            network_utilization_change = network_window[-1] - network_window[0]
            if network_utilization_change == 0:
                await destroy_container(container_id)
                await docker.close()
                break

        asyncio.sleep(freq)
    return 1


async def monitor():
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
    loop = asyncio.get_event_loop()
    loop.run_until_complete(monitor())
    loop.close()
