from sqlalchemy import Column, Integer, String, Numeric, Date, DateTime, func
from config.database import Base


class SpimexTradingResults(Base):
    __tablename__ = "spimex_trading_results"

    id = Column(Integer, primary_key=True)
    exchange_product_id = Column(String)
    exchange_product_name = Column(String)
    oil_id = Column(String(4))
    delivery_basis_id = Column(String(3))
    delivery_basis_name = Column(String)
    delivery_type_id = Column(String(1))
    volume = Column(Numeric(20, 2))
    total = Column(Numeric(20, 2))
    count = Column(Integer)
    date = Column(Date)
    created_on = Column(DateTime, server_default=func.now())
    updated_on = Column(
        DateTime,
        server_default=func.now(),
        onupdate=func.now()
        )
