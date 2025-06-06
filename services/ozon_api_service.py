import aiohttp

from utils.exc import OzonAPICrashError
import config


class OzonAPIService:

    async def get_product_data(
        self, short_link: str, del_zone: str | None = None
    ) -> str:
        url = f"{config.OZON_API_URL}/product/"
        if del_zone:
            url += f"{del_zone}/"

        url += short_link
        return await self.__make_get_response(url)

    async def get_delivery_zone(self, city_index: str) -> str:
        url = f"{config.OZON_API_URL}/pickUpPoint/{city_index}"
        return await self.__make_get_response(url)

    async def __make_get_response(self, url: str) -> str:
        timeout = aiohttp.ClientTimeout(total=config.API_SERVICES_TIMEOUT)
        async with aiohttp.ClientSession() as aiosession:
            try:
                async with aiosession.get(url=url, timeout=timeout) as response:
                    _status_code = response.status
                    print(f"OZON RESPONSE CODE {_status_code}")

                    if _status_code != 200:
                        raise OzonAPICrashError()

                    return await response.text()
            except TimeoutError as e:
                raise OzonAPICrashError() from e
