from pydantic import BaseModel


class ProductDTO(BaseModel):
    short_link: str
    name: str
    actual_price: int
    start_price: int
    basic_price: int
    photo_url: str | None
