from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from typing import List

app = FastAPI()


# Dominio denetatik hartzeko
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



    
class Jokalariak(BaseModel):
    id: int
    izena: str
    abizena: str
    nan: str
    puntuaketa: int
    denbora: int

# NAN Postgre datu base barruan existitzen al den konprobatu
def check_nan_exists(postgres_cursor, nan_value):
    postgres_cursor.execute("SELECT COUNT(*) FROM txapelketa_txapelketa WHERE NAN = %s", (nan_value,))
    count = postgres_cursor.fetchone()[0]
    return count > 0


# NAN postgreSQL barruan existitzen bada, denbora eta puntuaketa datuak aktualizatu
def update_nan_data(postgres_cursor, row):
    postgres_cursor.execute("UPDATE txapelketa_txapelketa SET denbora = %s, puntuaketa = %s WHERE NAN = %s",
                            (row[3], row[4], row[2]))

# NAN postgreSQL barruan ez bada existitzen, insert bat egin, datua berriak sartu ahal izateko
def insert_nan_data(postgres_cursor, row):
    postgres_cursor.execute("INSERT INTO txapelketa_txapelketa (izena, abizena, nan, denbora, puntuaketa) VALUES (%s, %s, %s, %s, %s)",
                            (row[0], row[1], row[2], row[3], row[4]))
    
    

from fastapi import FastAPI, HTTPException

class Ranking(BaseModel):
    id: int
    izena: str
    abizena: str
    nan: str
    puntuaketa: int
    denbora: int

# SQLiteko datua postgreSQLra pasatzeko funtzioa
@app.post('/datuak_berritu')
async def datuak_transferentzia(ranking_list: List[Ranking]):
    try:
        # Postgres datu basera konexioa egin
        postgres_conn = psycopg2.connect(
            database="st_db",
            user="odoo",
            password="odoo",
            host="10.23.28.192",
            port="5434"
        )
        
        postgres_cursor = postgres_conn.cursor()

        # Insertar datos en PostgreSQL
        for ranking in ranking_list:
            
            # Konprobatu existitzen al den, datu base barruan DNI hori
            kantitatea = check_nan_exists(postgres_cursor, ranking.nan)

            
            
            if(kantitatea > 0):
                
                # Datu base barruan existitzen bada, update egin datuei
                update_nan_data(postgres_cursor, (ranking.izena, ranking.abizena, ranking.nan, ranking.denbora, ranking.puntuaketa))
                
            else: 
                
                # Existitzen ez bada datu base barruan, insert egin
                insert_nan_data(postgres_cursor, (ranking.izena, ranking.abizena, ranking.nan, ranking.denbora, ranking.puntuaketa))
                


        # Commit para guardar los datos
        postgres_conn.commit()

        # Cerrar la conexión
        postgres_conn.close()

        return JSONResponse(content={"Mezua": "Datuak berritu dira."}, status_code=200)

    except HTTPException as http_exc:
        # Manejar específicamente la excepción HTTPException
        print(f"HTTPException: {http_exc}")
        raise http_exc

    except Exception as e:
        # Manejar otras excepciones
        print(f"Exception: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# PostgreSQLko datuak lortu MVC barruan irakusteko
@app.get('/lortu_datuak', response_model=List[Jokalariak])
async def lortu_datuak():
    try:
        # Postgres datu basera konexioa egin
        postgres_conn = psycopg2.connect(
            database="st_db",
            user="odoo",
            password="odoo",
            host="10.23.28.192",
            port="5434"
        )
        
        postgres_cursor = postgres_conn.cursor()

        # Postgres-etik datuak irakurri
        postgres_cursor.execute("SELECT id, izena, abizena, nan, puntuaketa, denbora FROM txapelketa_txapelketa order by puntuaketa desc")
        data = postgres_cursor.fetchall()
        
        

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # Konexioa itxi
        postgres_conn.close()

    return JSONResponse(content={"Jokalariak": data}, status_code=200)


@app.get("/ping")
def ping():
    return {"message": "¡API ondo dabil, OK!"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8012)
