import config
from db.base import Product, Punkt

from logger import logger

from services.ozon.ozon_api_service import OzonAPIService
from services.wb.wb_api_service import WbAPIService


async def get_product_price(product: Product, punkt: Punkt | None) -> int | None:
    if product.product_marker == "ozon":
        return await get_ozon_product_price(product, punkt)

    return await get_wb_product_price(product, punkt)


async def get_ozon_product_price(product: Product, punkt: Punkt | None) -> int | None:
    zone = punkt.ozon_zone if punkt else None

    product.name = product.name if product.name else "Отсутствует"
    try:
        api_service = OzonAPIService()
        res = await api_service.get_product_data(product.short_link, zone)
        data = api_service.parse_product_data(res)
    except Exception:
        logger.error(
            "Can't get product price for ozon product %s for zone %s", product.id, zone
        )
        return None

    return data.actual_price


async def get_wb_product_price(product: Product, punkt: Punkt | None) -> int | None:
    zone = punkt.wb_zone if punkt else config.WB_DEFAULT_DELIVERY_ZONE

    try:
        api_service = WbAPIService()
        res = await api_service.get_product_data(product.short_link, zone)
        data = api_service.parse_product_data(res)
    except Exception:
        logger.error(
            "Can't get product price for wb product %s for zone %s", product.id, zone
        )
        return None

    return data.actual_price
