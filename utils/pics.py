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
