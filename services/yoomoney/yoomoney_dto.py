from datetime import datetime

from pydantic import BaseModel


class YoomoneyNotificationData(BaseModel):
    """
    `notification_type` - `string` - Для переводов из кошелька — p2p-incoming.
    Для переводов с произвольной карты — card-incoming.\n
    `operation_id` - `string` - Идентификатор операции в истории вашего кошелька.\n
    `amount` - `amount` - Сумма, которая зачислена на баланс вашего кошелька.\n
    `withdraw_amount` - `amount` - Сумма, которую перевел отправитель и которую списали
    с баланса его кошелька или с карты.\n
    `currency` - `string` - Код валюты — всегда 643 (рубль РФ согласно ISO 4217).\n
    `datetime` - `datetime` - Дата и время совершения перевода.\n
    `sender` - `string` - Для переводов из кошелька — номер кошелька отправителя.
    Для переводов с произвольной карты — параметр содержит пустую строку.\n
    `codepro` - `boolean` - Признак того, что перевод защищен кодом протекции.
    В ЮMoney больше нельзя делать переводы с кодом протекции, 
    поэтому параметр всегда имеет значение false.\n
    `label` - `string` - Метка платежа. Если ее нет, параметр содержит пустую строку.\n
    `sha1_hash` - `string` - SHA-1 hash параметров уведомления.\n
    `unaccepted` - `boolean` - Признак того, что кошелек достиг лимита доступного остатка,
    перевод захолдирован (заморожен) до тех пор, пока пользователь не освободит место в кошельке.
    В ЮMoney больше нельзя делать переводы с холдированием, 
    поэтому параметр всегда имеет значение false.
    """

    notification_type: str
    operation_id: str
    amount: float
    withdraw_amount: float
    currency: str
    datetime: datetime
    sender: str
    codepro: bool
    label: str
    sha1_hash: str
    unaccepted: bool
