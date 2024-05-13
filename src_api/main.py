from fastapi import FastAPI, Response, status, HTTPException
from fastapi.responses import PlainTextResponse

from helpers_api import create_db_connection, is_valid_advertiser
from constants_api import model_col_names
import pandas as pd

from dotenv import load_dotenv

load_dotenv()

app = FastAPI()


@app.get("/", response_class=PlainTextResponse)
def read_root():
    return """
        Entradas API

        -/recommendations/<ADV>/<Modelo>
        Esta entrada devuelve un JSON en donde se indican las recomendaciones del
        día para el adv y el modelo en cuestión.
            Modelos posibles:
                - top_ctr: TOP CTR
                - top_product: TOP PRODUCT

        -/stats/
        Esta entrada devuelve un JSON con un resumen de estadísticas sobre coincidencias en las 
        recomendaciones de ambos modelos para los diferentes advs. y fechas.
        Para cada advertiser (activo) devuelve el porcentaje de productos coincidentes por
        fecha. Si alguna fecha no existe para un advertiser es que ese día no hubo coindicencias
        entre los modelos.

        -/history/<ADV>/
        Esta entrada devuelve un JSON con todas las recomendaciones para el
        advertiser pasado por parámetro en los últimos 7 días.
        """


@app.get("/recommendations/{advertiser}/{model}")
def recommendations(advertiser: str, model: str):

    if model not in model_col_names.keys():
        raise HTTPException(status_code=404, detail="Model not found")

    if not is_valid_advertiser(advertiser=advertiser, model=model):
        raise HTTPException(
            status_code=404,
            detail="Advertiser does not exist or is not available for current date",
        )

    query = """
        SELECT product_id
        FROM public.{model_table}
        WHERE date = (SELECT MAX(date) FROM public.{model_table})
        AND advertiser_id = '{advertiser}'
        ORDER BY {col} desc
    """
    conn = create_db_connection()
    df = pd.read_sql_query(
        sql=query.format(
            model_table=model, col=model_col_names.get(model), advertiser=advertiser
        ),
        con=conn,
    )
    return df.to_dict(orient="records")


@app.get("/history/{advertiser}")
def history(advertiser: str):
    query = """
        SELECT 'top_ctr' model, date, product_id
        FROM top_ctr
        WHERE advertiser_id = %s
        AND date BETWEEN (CURRENT_DATE - INTERVAL '8 days') and (CURRENT_DATE - INTERVAL '1 day') 
        UNION
        SELECT 'top_product' model, date, product_id
        FROM top_product
        WHERE advertiser_id = %s
        AND date BETWEEN (CURRENT_DATE - INTERVAL '8 days') and (CURRENT_DATE - INTERVAL '1 day')
    """
    conn = create_db_connection()
    df = pd.read_sql_query(sql=query, con=conn, params=(advertiser, advertiser))

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="Advertiser does not exist or does not have any registers in the last 7 days",
        )

    return df.to_dict(orient="records")


@app.get("/stats")
def stats():
    query = """
        WITH joined_models as (
            SELECT 	
                ctr.date, 
                ctr.advertiser_id, 
                TO_CHAR(ROUND((CAST(COUNT(ctr.product_id) as NUMERIC))/20,2), 'FM999999990.00') coincidence_pctg
            FROM top_ctr ctr
            INNER JOIN top_product prod 
                ON ctr.date = prod.date 
                AND ctr.advertiser_id = prod.advertiser_id 
                AND ctr.product_id = prod.product_id
        GROUP BY ctr.date, ctr.advertiser_id 
        )
        SELECT advertiser_id, date, coincidence_pctg
        FROM joined_models
        ORDER BY 1, 2 , 3
    """
    conn = create_db_connection()
    df = pd.read_sql_query(sql=query, con=conn)

    if df.empty:
        raise HTTPException(
            status_code=404,
            detail="There are no coincidences between models for any dates",
        )

    return df.to_dict(orient="records")
