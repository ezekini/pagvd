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
        Esta entrada devuelve un JSON con un resumen de estadísticas sobre las
        recomendaciones a determinar por ustedes. Algunas opciones posibles:
        ● Cantidad de advertisers
        ● Advertisers que más varían sus recomendaciones por día
        ● Estadísticas de coincidencia entre ambos modelos para los diferentes
        advs.

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
            status_code=404, detail="Advertiser not available for current date"
        )

    query = """
        SELECT product_id
        FROM public.{model_table}
        WHERE date = (SELECT MAX(date) FROM public.{model_table})
        ORDER BY {col} desc
    """
    conn = create_db_connection()
    df = pd.read_sql_query(
        sql=query.format(model_table=model, col=model_col_names.get(model)), con=conn
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
            detail="Advertiser doesn't exist or isn't present in the last 7 days",
        )

    return df.to_dict(orient="records")
