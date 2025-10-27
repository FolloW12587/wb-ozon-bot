from pydantic import BaseModel


class ProductDTO(BaseModel):
    actual_price: int
    basic_price: int
