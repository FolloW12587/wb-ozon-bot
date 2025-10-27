import base64
import json
import aiohttp

from services.wb.dto import ProductDTO
from utils.exc import WbAPICrashError
import config


class WbAPIService:

    async def get_product_data(self, short_link: str, del_zone: str) -> dict:
        url = f"{config.WB_API_URL}/product/{del_zone}/{short_link}"
        return json.loads(await self.__make_get_response(url))

    async def get_delivery_zone(self, city_index: str) -> str:
        url = f"{config.WB_API_URL}/pickUpPoint/{city_index}"
        return await self.__make_get_response(url)

    async def get_product_image(self, short_link: str) -> bytes:
        url = f"{config.WB_API_URL}/product/image/{short_link}"
        image_str = await self.__make_get_response(url)
        return base64.b64decode(image_str)

    async def __make_get_response(self, url: str) -> str:
        timeout = aiohttp.ClientTimeout(total=config.API_SERVICES_TIMEOUT)
        async with aiohttp.ClientSession() as aiosession:
            try:
                async with aiosession.get(url=url, timeout=timeout) as response:
                    _status_code = response.status
                    print(f"WB RESPONSE CODE {_status_code}")

                    if _status_code != 200:
                        _text = await response.text()
                        raise WbAPICrashError(
                            f"Status code is not 200 {_status_code}. Text: {_text}"
                        )

                    return await response.text()
            except TimeoutError as e:
                raise WbAPICrashError("Timeout api") from e

    def parse_product_data(self, data: dict):
        d = data.get("data")

        sizes = d.get("products")[0].get("sizes")

        basic_price = product_price = None

        for size in sizes:
            _price = size.get("price")

            if _price:
                basic_price = size.get("price").get("basic")
                product_price = size.get("price").get("product")

                basic_price = str(basic_price)[:-2]
                product_price = str(product_price)[:-2]

        return ProductDTO(actual_price=product_price, basic_price=basic_price)
