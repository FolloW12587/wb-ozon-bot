from sqlalchemy.ext.automap import automap_base
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker

from config import db_url, _db_url

import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker, declarative_base, relationship
from sqlalchemy import Column, Integer, String, DATETIME, ForeignKey, Float, DateTime, TIMESTAMP, BLOB, JSON, BigInteger, Table


# Base = declarative_base()
Base = automap_base()


class Subscription(Base):
    __tablename__ = 'subscriptions'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    wb_product_limit = Column(Integer)
    ozon_product_limit = Column(Integer)
    
    users = relationship('User', back_populates="subscription")


class User(Base):
    __tablename__ = 'users'
    
    tg_id = Column(BigInteger, primary_key=True, index=True)
    username = Column(String, nullable=True)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)
    time_create = Column(TIMESTAMP(timezone=True))
    last_action = Column(String, nullable=True, default=None)
    last_action_time = Column(TIMESTAMP(timezone=True), nullable=True, default=None)
    subscription_id = Column(BigInteger, ForeignKey('subscriptions.id'), nullable=True, default=None)
    utm_source = Column(String, nullable=True, default=None)
    #
    wb_total_count = Column(Integer, default=0)
    ozon_total_count = Column(Integer, default=0)
    #
    subscription = relationship(Subscription, back_populates="users")
    ozon_products = relationship("OzonProduct", back_populates="user")
    wb_punkts = relationship("WbPunkt", back_populates="user")
    wb_products = relationship("WbProduct", back_populates="user")
    jobs = relationship('UserJob', back_populates="user")
    utm = relationship("UTM", uselist=False, back_populates="user")  # Связь OneToOne


class UTM(Base):
    __tablename__ = 'utms'

    id = Column(Integer, primary_key=True, index=True)
    keitaro_id = Column(String)
    utm_source = Column(String, nullable=True, default=None)
    utm_medium = Column(String, nullable=True, default=None)
    utm_campaign = Column(String, nullable=True, default=None)
    utm_content = Column(String, nullable=True, default=None)
    utm_term = Column(String, nullable=True, default=None)
    banner_id = Column(String, nullable=True, default=None)
    campaign_name = Column(String, nullable=True, default=None)
    campaign_name_lat = Column(String, nullable=True, default=None)
    campaign_type = Column(String, nullable=True, default=None)
    campaign_id = Column(String, nullable=True, default=None)
    creative_id = Column(String, nullable=True, default=None)
    device_type = Column(String, nullable=True, default=None)
    gbid = Column(String, nullable=True, default=None)
    keyword = Column(String, nullable=True, default=None)
    phrase_id = Column(String, nullable=True, default=None)
    coef_goal_context_id = Column(String, nullable=True, default=None)
    match_type = Column(String, nullable=True, default=None)
    matched_keyword = Column(String, nullable=True, default=None)
    adtarget_name = Column(String, nullable=True, default=None)
    adtarget_id = Column(String, nullable=True, default=None)
    position = Column(String, nullable=True, default=None)
    position_type = Column(String, nullable=True, default=None)
    source = Column(String, nullable=True, default=None)
    source_type = Column(String, nullable=True, default=None)
    region_name = Column(String, nullable=True, default=None)
    region_id = Column(String, nullable=True, default=None)
    yclid = Column(String, nullable=True, default=None)
    client_id = Column(String, nullable=True, default=None)

    user_id = Column(BigInteger, ForeignKey('users.tg_id'), nullable=True, unique=True)  # Внешний ключ с уникальным ограничением
    user = relationship("User", back_populates="utm")  # Связь OneToOne


class Punkt(Base):
    __tablename__ = 'punkts'
    
    id = Column(Integer, primary_key=True, index=True)
    # lat = Column(Float)
    # lon = Column(Float)
    index = Column(BigInteger)
    city = Column(String)
    wb_zone = Column(BigInteger, default=None, nullable=True)
    ozon_zone = Column(BigInteger, default=None, nullable=True)
    time_create = Column(TIMESTAMP(timezone=True))
    user_id = Column(BigInteger, ForeignKey('users.tg_id'), nullable=True)
    
    # Связь с пользователем
    user = relationship(User, back_populates="punkts")
    # wb_products = relationship('WbProduct', back_populates="punkt")
    # ozon_products = relationship('OzonProduct', back_populates="punkt")


class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True, index=True)
    product_marker = Column(String)
    name = Column(String, nullable=True)
    short_link = Column(String, unique=True)
    seller = Column(String, nullable=True)
    rate = Column(String, nullable=True)
    photo_id = Column(String, nullable=True)


class UserProduct(Base):
    __tablename__ = 'user_products'

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(BigInteger, ForeignKey('products.id'))
    user_id = Column(BigInteger, ForeignKey('users.tg_id'))
    link = Column(String)
    start_price = Column(Integer)
    actual_price = Column(Integer)
    sale = Column(Integer)
    time_create = Column(TIMESTAMP(timezone=True))
    last_send_price = Column(Integer, nullable=True, default=None)

    user = relationship(User, back_populates="products")
    product = relationship(Product, back_populates="user_products")
    user_product_jobs = relationship('UserProductJob', back_populates="user_product")


class ProductPrice(Base):
    __tablename__ = 'product_prices'

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(BigInteger, ForeignKey('products.id'))
    price = Column(Integer)
    time_price = Column(TIMESTAMP(timezone=True))
    city = Column(String)

    product = relationship(Product, back_populates="product_prices")


class ProductCityGraphic(Base):
    __tablename__ = 'product_city_graphics'

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(BigInteger, ForeignKey('products.id'))
    time_create = Column(TIMESTAMP(timezone=True))
    city = Column(String)
    photo_id = Column(String, nullable=True, default=None)

    product = relationship(Product, back_populates="product_city_graphic")


class UserProductJob(Base):
    __tablename__ = 'user_product_job'

    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String)
    user_product_id = Column(Integer, ForeignKey('user_products.id'))

    user_product = relationship(UserProduct, back_populates="user_product_jobs")


category_channel_association = Table(
    'category_channel_association',
    Base.metadata,
    Column('id', Integer, primary_key=True),    
    Column('category_id', Integer, ForeignKey('categories.id'), index=True),
    Column('channel_link_id', Integer, ForeignKey('channel_links.id'), index=True)
)


class Category(Base):
    __tablename__ = 'categories'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    parent_id = Column(BigInteger, ForeignKey('categories.id'), nullable=True, default=None)

    parent = relationship('Category',remote_side=[id], back_populates="child_categories")
    child_categories = relationship('Category', back_populates="parent")

    channel_links = relationship(
        'ChannelLink',
        secondary=category_channel_association,
        back_populates='categories'
    )


class ChannelLink(Base):
    __tablename__ = 'channel_links'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    channel_id = Column(String)

    categories = relationship(
        'Category',
        secondary=category_channel_association,
        back_populates='channel_links'
    )


class PopularProduct(Base):
    __tablename__ = 'popular_products'

    id = Column(Integer, primary_key=True, index=True)
    product_id = Column(BigInteger, ForeignKey('products.id'))
    category_id = Column(BigInteger, ForeignKey('categories.id'))
    start_price = Column(Integer)
    actual_price = Column(Integer)
    sale = Column(Integer)
    link = Column(String)
    time_create = Column(TIMESTAMP(timezone=True))

    product = relationship(Product, back_populates="popular_products")
    category = relationship(Category, back_populates="popular_products")


class WbPunkt(Base):
    __tablename__ = 'wb_punkts'
    
    id = Column(Integer, primary_key=True, index=True)
    # lat = Column(Float)
    # lon = Column(Float)
    index = Column(BigInteger)
    city = Column(String)
    zone = Column(BigInteger, default=None, nullable=True)
    time_create = Column(TIMESTAMP(timezone=True))
    user_id = Column(BigInteger, ForeignKey('users.tg_id'), nullable=True)
    
    # Связь с пользователем
    user = relationship(User, back_populates="wb_punkts")
    wb_products = relationship('WbProduct', back_populates="wb_punkt")


class OzonPunkt(Base):
    __tablename__ = 'ozon_punkts'
    
    id = Column(Integer, primary_key=True, index=True)
    # lat = Column(Float)
    # lon = Column(Float)
    index = Column(BigInteger)
    city = Column(String)
    zone = Column(BigInteger, default=None, nullable=True)
    time_create = Column(TIMESTAMP(timezone=True))
    user_id = Column(BigInteger, ForeignKey('users.tg_id'), nullable=True)
    
    # Связь с пользователем
    user = relationship(User, back_populates="ozon_punkts")
    ozon_products = relationship('OzonProduct', back_populates="ozon_punkt")


class OzonProduct(Base):
    __tablename__ = 'ozon_products'
    
    id = Column(Integer,
                primary_key=True,
                index=True)
    link = Column(String)
    short_link = Column(String)
    basic_price = Column(Float)
    start_price = Column(Float)
    actual_price = Column(Float)
    sale = Column(Float)
    # percent = Column(Integer)
    name = Column(String,
                  nullable=True,
                  default=None)
    # expected_price = Column(Float)
    # username = Column(String, nullable=True)
    # first_name = Column(String, nullable=True)
    # last_name = Column(String, nullable=True)
    time_create = Column(TIMESTAMP(timezone=True))
    user_id = Column(BigInteger, ForeignKey('users.tg_id'))
    ozon_punkt_id = Column(Integer, ForeignKey('ozon_punkts.id', ondelete='SET NULL'), nullable=True)
    
    # Связь с пользователем
    user = relationship(User, back_populates="ozon_products")
    ozon_punkt = relationship(OzonPunkt, back_populates="ozon_products", passive_deletes=True)



class WbProduct(Base):
    __tablename__ = 'wb_products'
    
    id = Column(Integer,
                primary_key=True,
                index=True)
    link = Column(String)
    short_link = Column(String)
    start_price = Column(Float)
    actual_price = Column(Float)
    sale = Column(Float)
    name = Column(String,
                  nullable=True,
                  default=None)
    time_create = Column(TIMESTAMP(timezone=True))
    user_id = Column(BigInteger, ForeignKey('users.tg_id'))
    wb_punkt_id = Column(Integer, ForeignKey('wb_punkts.id', ondelete='SET NULL'), nullable=True)

    user = relationship(User, back_populates="wb_products")
    wb_punkt = relationship(WbPunkt, back_populates="wb_products", passive_deletes=True)



class UserJob(Base):
    __tablename__ = 'user_job'
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(String)
    user_id = Column(BigInteger, ForeignKey('users.tg_id'))
    product_id = Column(Integer)
    product_marker = Column(String)

    user = relationship(User, back_populates="jobs")


# Создаем асинхронный движок и сессию
# DATABASE_URL = "sqlite+aiosqlite:///test.db"

sync_engine = create_engine(_db_url, echo=True)



# Base.prepare(engine, reflect=True)
Base.prepare(autoload_with=sync_engine)
# Base.metadata.reflect(bind=sync_engine)

engine = create_async_engine(db_url, echo=True)
session = async_sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
# AsyncSessionLocal = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


async def get_session():
    async with session() as _session:
            yield _session

# Base = automap_base()

# engine = create_engine(db_url,
#                        echo=True)

# # Base.prepare(engine, reflect=True)
# Base.prepare(autoload_with=engine)

# session = sessionmaker(engine, expire_on_commit=False)