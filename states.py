from aiogram.fsm.state import StatesGroup, State


class SwiftSepaStates(StatesGroup):
    coords = State()


class FeedbackFormStates(StatesGroup):
    reason = State()
    description = State()
    contact = State()
    username = State()


class ProductStates(StatesGroup):
    _id = State()
    percent = State()


class AnyProductStates(StatesGroup):
    link = State()


class OzonProduct(StatesGroup):
    product = State()
    percent = State()



class EditSale(StatesGroup):
    new_sale = State()


class NewEditSale(StatesGroup):
    new_sale = State()


class LocationState(StatesGroup):
    location = State()


class PunktState(StatesGroup):
    city = State()