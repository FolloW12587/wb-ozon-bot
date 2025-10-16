import json
import re
import aiohttp

from utils.exc import OzonAPICrashError, OzonAPIAttemptsExceeded
import config

from services.ozon.dto import ProductDTO


class OzonAPIService:

    async def get_product_data(
        self, short_link: str, del_zone: str | None = None
    ) -> str:
        url = f"{config.OZON_API_URL}/product/"
        if del_zone:
            url += f"{del_zone}/"

        url += short_link
        attempt = 1
        while attempt < 4:
            try:
                return await self.__make_get_response(url)
            except Exception:
                attempt += 1
                if attempt > 3:
                    raise

        raise OzonAPIAttemptsExceeded()

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
                        _text = await response.text()
                        raise OzonAPICrashError(
                            f"Status code is not 200 {_status_code}. Text: {_text}"
                        )

                    return await response.text()
            except TimeoutError as e:
                raise OzonAPICrashError("API timeout occured") from e

    def parse_product_data(self, raw_data: str) -> ProductDTO:
        short_link = raw_data.split("|")[0]

        response_data = raw_data.split("|", maxsplit=1)[-1]
        photo_url = self.get_photo_url_pattern(raw_data)

        w = re.findall(r"\"cardPrice.*currency?", raw_data)

        json_data: dict = json.loads(response_data)
        name = " ".join(json_data.get("seo").get("title").split()[:4])

        if w:
            w = w[0].split(",")[:3]

            _d = {
                "price": None,
                "originalPrice": None,
                "cardPrice": None,
            }

            for k in _d:
                if all(v for v in _d.values()):
                    break

                for q in w:
                    if q.find(k) == -1:
                        continue

                    _, price = q.split(":")
                    price = price.replace("\\", "").replace('"', "")
                    price = float("".join(price.split()[:-1]))

                    _d[k] = price
                    break

            return ProductDTO(
                short_link=short_link,
                name=name,
                actual_price=int(_d.get("cardPrice", 0)),
                start_price=int(_d.get("cardPrice", 0)),
                basic_price=int(_d.get("price", 0)),
                photo_url=photo_url,
            )

        script_list = json_data.get("seo").get("script")

        inner_html: dict = json.loads(script_list[0].get("innerHTML"))
        price = inner_html.get("offers").get("price")

        return ProductDTO(
            short_link=short_link,
            name=name,
            actual_price=int(price),
            start_price=int(price),
            basic_price=int(price),
            photo_url=photo_url,
        )

    def shorten_link(self, link: str) -> str:
        if link.startswith("https://ozon.ru/t/"):
            _idx = link.find("/t/")
            _prefix = "/t/"
            return "croppedLink|" + link[_idx + len(_prefix) :]

        _prefix = "product/"
        _idx = link.rfind("product/")
        return link[(_idx + len(_prefix)) :]

    def get_photo_url_pattern(self, raw_product_data: str) -> str | None:
        photo_url_pattern = r'images\\":\[{\\"src\\":\\"https:\/\/cdn1\.ozone\.ru\/s3\/multimedia-[a-z0-9]*(-\w*)?\/\d+\.jpg'

        match = re.search(photo_url_pattern, raw_product_data)

        if not match:
            return None

        photo_url_match = re.search(r"https.*\.jpg?", match.group())
        if not photo_url_match:
            return None

        return photo_url_match.group()
