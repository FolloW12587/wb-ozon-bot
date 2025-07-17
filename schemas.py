from enum import Enum
import json
import os
from pydantic import BaseModel, RootModel, Field

import config
from custom_types import Markup


class UTMSchema(BaseModel):
    keitaro_id: str | None = Field(default=None, alias="user")
    utm_source: str | None = Field(default=None)
    utm_medium: str | None = Field(default=None)
    utm_campaign: str | None = Field(default=None)
    utm_content: str | None = Field(default=None)
    utm_term: str | None = Field(default=None)
    banner_id: str | None = Field(default=None)
    campaign_name: str | None = Field(default=None)
    campaign_name_lat: str | None = Field(default=None)
    campaign_type: str | None = Field(default=None)
    campaign_id: str | None = Field(default=None)
    creative_id: str | None = Field(default=None)
    device_type: str | None = Field(default=None)
    gbid: str | None = Field(default=None)
    keyword: str | None = Field(default=None)
    phrase_id: str | None = Field(default=None)
    coef_goal_context_id: str | None = Field(default=None)
    match_type: str | None = Field(default=None)
    matched_keyword: str | None = Field(default=None)
    adtarget_name: str | None = Field(default=None)
    adtarget_id: str | None = Field(default=None)
    position: str | None = Field(default=None)
    position_type: str | None = Field(default=None)
    source: str | None = Field(default=None)
    source_type: str | None = Field(default=None)
    region_name: str | None = Field(default=None)
    region_id: str | None = Field(default=None)
    yclid: str | None = Field(default=None)
    client_id: str | None = Field(default=None)


class FAQQuestion(str, Enum):
    ADD_PRODUCT = "add_product"
    VIEW_PRODUCT = "view_product"
    EDIT_SALE_PRODUCT = "edit_sale_product"
    DELETE_PRODUCT = "delete_product"
    SEND_PUSH_PRODUCT = "send_push_product"
    COUNTRY_PRODUCT = "country_product"


class FAQPicDict(RootModel[dict[str, list[str]]]):
    def get(self, key: FAQQuestion) -> list[str]:
        return self.root.get(key.value, [])

    def set(self, key: FAQQuestion, value: list[str]):
        self.root[key.value] = value


class ImageConfig(BaseModel):
    default_product_photo: str = ""
    default_product_list_photo: str = ""
    start_pic: str = ""
    faq_pic_dict: FAQPicDict = FAQPicDict({})

    @classmethod
    def load(cls) -> "ImageConfig":
        path = config.IMAGES_CONFIG_PATH
        if not os.path.exists(path):
            # создаём дефолтный пустой конфиг
            default_config = cls()
            with open(path, "w", encoding="utf-8") as f:
                json.dump(default_config.model_dump(), f, indent=2)
            return default_config

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return cls(**data)

    def save(self):
        path = config.IMAGES_CONFIG_PATH
        with open(path, "w", encoding="utf-8") as f:
            json.dump(self.model_dump(), f, indent=2)


class MessageInfo(BaseModel):
    text: str
    markup: Markup | None = None
