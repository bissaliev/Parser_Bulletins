from database.models import SpimexTradingResults
from sqlalchemy import insert
from sqlalchemy.orm import Session


def create_trade(session: Session, **data):
    query = insert(SpimexTradingResults).values(**data)
    session.execute(query)
    session.commit()


def mass_create_trade(session: Session, lst_data: list):
    session.execute(insert(SpimexTradingResults), lst_data)
