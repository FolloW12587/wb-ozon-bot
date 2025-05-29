import re
import os

from aiogram import Bot, types

import config
from schemas import ImageConfig, FAQQuestion


class ImageManager:
    def __init__(self, bot: Bot):
        self.bot = bot
        self.config = ImageConfig.load()
        self.images_dir = config.IMAGES_DIR

    def save(self):
        self.config.save()

    async def get_start_pic_id(self) -> str | None:
        if self.config.start_pic:
            return self.config.start_pic

        path = os.path.join(self.images_dir, "start_pic.jpg")
        if not os.path.exists(path):
            return None

        photo_id = await self._generate_photo_id(path)
        self.config.start_pic = photo_id
        self.save()
        return photo_id

    async def get_faq_photo_ids(self, question: FAQQuestion) -> list[str]:
        photo_ids = self.config.faq_pic_dict.get(question)
        if photo_ids:
            return photo_ids

        photo_ids = []
        paths = self._get_question_images(question.value)
        for path in paths:
            photo_id = await self._generate_photo_id(path)
            photo_ids.append(photo_id)

        self.config.faq_pic_dict.set(question, photo_ids)
        self.save()
        return photo_ids

    async def _generate_photo_id(self, path: str) -> str:
        photo = types.FSInputFile(path)
        msg = await self.bot.send_photo(chat_id=config.DUMP_CHAT, photo=photo)
        file_id = msg.photo[-1].file_id

        return file_id

    def _get_question_images(self, question: str) -> list[str]:
        """Собирает локальные пути по шаблону question_n.jpg"""
        pattern = re.compile(rf"{re.escape(question)}_(\d+)\.jpe?g", re.IGNORECASE)
        files = []

        for fname in os.listdir(self.images_dir):
            match = pattern.match(fname)
            if match:
                files.append(
                    (int(match.group(1)), os.path.join(self.images_dir, fname))
                )

        files.sort()  # сортировка по номеру
        return [path for _, path in files]


# start_pic = 'AgACAgIAAxkBAAIEg2fIRzHfgJ-hleejB6EEV-VVGX-EAAJ_6jEbG0JBSqChanbUZTMRAQADAgADcwADNgQ'
# start_pic = 'AgACAgIAAxkBAAIF2WfO2shdrxWbscxMII2DJuN7-EARAAIC7DEbNpZ4SojgRlh3FzMSAQADAgADcwADNgQ'
start_pic = (
    "AgACAgIAAxkBAAMVaDPY4TQ9vmTMiEtlejXdTtNn1hIAAqMDMhtekJlJPMmac6oHxWoBAAMCAAN5AAM2BA"
)

# DEFAULT_PRODUCT_PHOTO_ID = 'AgACAgIAAxkBAAIpBmfzw-B9QjnbYbvbDmD3Ggq2aTkbAAK76zEb3wigS7IYUdYuGf5PAQADAgADcwADNgQ'
DEFAULT_PRODUCT_PHOTO_ID = "AgACAgIAAxkBAAIzz2f3v0G9jTjd75BYjWO18-_hZWMpAAJa6zEbKZnBS8bKQlhA18YAAQEAAwIAA3MAAzYE"

DEFAULT_PRODUCT_LIST_PHOTO_ID = "AgACAgIAAxkBAAIpBWfzw6GOSEEff1XS2586bT6I9mgWAAKG7zEbJrGYSz8aSxgrOfLVAQADAgADcwADNgQ"

faq_pic_dict = {
    "add_product": [
        "AgACAgIAAxkBAAIC82fHEta81X3SkdKQVVBcF5rT52HdAAJX6jEbtyc5SpHo321SsS2JAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIC9GfHE1fATHv6uYlGoswXvEpsgjeWAAJa6jEbtyc5SjOOgrcj2ukFAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIDF2fIAAGYvHIX0AJFiKxbVbYC9C_d_wACtu0xGxJTQUooLQaC1TLk8wEAAwIAA3MAAzYE",
        "AgACAgIAAxkBAAIC9mfHE6XI97vKC-jNp2nsA5LBpKxUAAJP5TEbElM5SqBxPnk4ocLGAQADAgADcwADNgQ",
    ],
    "view_product": [
        "AgACAgIAAxkBAAIEhGfISEUYkEohATzkjXrMScxR8jtOAAKV6jEbG0JBSoLC1eyVxlOPAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEhWfISG1grPd7f3c1ml1U87vq7E-gAAI17zEbElNJSlW76-I6xn5bAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEhmfISIyh4yiODoX1PaAg_Uo4P5FgAAKX6jEbG0JBSiPaYiwKeqZpAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEh2fISKxNG4UY46IILo0JphHxlMibAAKY6jEbG0JBSqi5aakoIu5XAQADAgADcwADNgQ",
    ],
    "edit_sale_product": [
        "AgACAgIAAxkBAAIEiGfISNA1lSjKVXV2XkjRSN-R45H1AAKZ6jEbG0JBSs-Ls0u439JpAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEiWfISP7QDMr_PzXI-NzgUXjmvCxPAAI87zEbElNJSqE4MpH4FBPRAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEimfISR48iJaV-s9AjRwr7DAcqwy7AAKf6jEbG0JBSog6YAr4XjiWAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEi2fISTzZ9C7_yl-S_qN9YqzSGZY9AAKj6jEbG0JBSrytY1IZwhBDAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEjGfISVyd6ZyaG2YAAUUVo-4oGKFgnQACpuoxGxtCQUprhq5ZXW8QjgEAAwIAA3MAAzYE",
    ],
    "delete_product": [
        "AgACAgIAAxkBAAIEjWfISZpQu50fh2-_ChNYDFZ6UNoHAAKn6jEbG0JBSqNU6L27lg0oAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEjmfISbS3UHpf-uV-K0pMQJc5fmV1AAKq6jEbG0JBSvFoadF9ydaoAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEj2fISc6YXBuj2o70qiiNQ-d458CsAAKs6jEbG0JBSlGmYlhKRMSCAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEkGfISfejmMarKP-4WWNnekDyESe6AAKt6jEbG0JBSo6LWDG0DNARAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEkWfIShbVSf_AqW5UgJDkf3ne4v-NAAKv6jEbG0JBSvRFLR2xkO0BAQADAgADcwADNgQ",
    ],
    "send_push_product": [
        "AgACAgIAAxkBAAIEkmfISk6jhRd4BmRUWXSnvjoNp9KLAAK06jEbG0JBSrFxR_2CzgHrAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIEk2fISntXMbwkqKWDJEUvtaal-iVUAAK26jEbG0JBSmGSnvUCfT0EAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIElGfISpe0XGmU7eoDGBBvhSwX-410AAJJ7zEbElNJSu1j5RpnbFuMAQADAgADcwADNgQ",
    ],
    "country_product": [
        "AgACAgIAAxkBAAIElWfISrlOsyhD6ZX65LHYrCVkJ9QSAAK36jEbG0JBSpP2qmikhjGYAQADAgADcwADNgQ",
        "AgACAgIAAxkBAAIElmfIStqFxFm7uZyDtNPj7d4ZqysEAAK66jEbG0JBSt1a5hI4m97QAQADAgADcwADNgQ",
    ],
}
