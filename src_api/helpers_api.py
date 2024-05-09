from os import environ
from sqlalchemy import create_engine, text


def create_db_connection():
    """
    Function that creates engine to db,
    """
    server = environ["DB_SERVER"]
    database = environ["DB_NAME"]
    user = environ["DB_USER"]
    password = environ["DB_PASS"]
    port = environ["DB_PORT"]

    # Create the SQLAlchemy engine
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{server}:{port}/{database}"
    )
    return engine


def is_valid_advertiser(advertiser: str, model: str) -> bool:
    valid = True
    validation_query = """
        SELECT advertiser_id
        FROM {model}
        WHERE date = (SELECT MAX(date) FROM {model})
        AND advertiser_id = '{advertiser}'
    """
    engine = create_db_connection()
    with engine.connect() as connection:
        result = connection.execute(
            text(validation_query.format(model=model, advertiser=advertiser))
        )
    if result.fetchone() == None:
        valid = False
    return valid
