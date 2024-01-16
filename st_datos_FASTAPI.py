from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import sqlite3
import psycopg2


app = FastAPI()


# Dominio denetatik hartzeko
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Item(BaseModel):
    ruta: str

# NAN Postgre datu base barruan existitzen al den konprobatu
def check_nan_exists(postgres_cursor, nan_value):
    postgres_cursor.execute("SELECT COUNT(*) FROM txapelketa_txapelketa WHERE NAN = %s", (nan_value,))
    count = postgres_cursor.fetchone()[0]
    return count > 0

# NAN postgreSQL barruan existitzen bada, denbora eta puntuaketa datuak aktualizatu
def update_nan_data(postgres_cursor, nan_value, denbora, puntuaketa):
    postgres_cursor.execute("UPDATE txapelketa_txapelketa SET denbora = %s, puntuaketa = %s WHERE NAN = %s",
                            (denbora, puntuaketa, nan_value))

# NAN postgreSQL barruan ez bada existitzen, insert bat egin, datua berriak sartu ahal izateko
def insert_nan_data(postgres_cursor, row):
    postgres_cursor.execute("INSERT INTO txapelketa_txapelketa (NAN, izena, abizena, denbora, puntuaketa) VALUES (%s, %s, %s, %s, %s)",
                            (row[0], row[1], row[2], row[3], row[4]))

# SQLiteko datua postgreSQLra pasatzeko funtzioa
@app.post('/datuak_berritu')
async def datuak_transferentzia(item: Item):
    
    try:
        
        # SQLite datu basera konexioa ireki
        sqlite_conn = sqlite3.connect(item.ruta)
        sqlite_cursor = sqlite_conn.cursor()

        # Postgres datu basera konexioa egin
        postgres_conn = psycopg2.connect(
            database="st_db",
            user="odoo",
            password="odoo",
            host="10.23.28.192",
            port="5434"
        )
        
        postgres_cursor = postgres_conn.cursor()

        # SQLiteko datuak hartu eta Postgresera pasatu
        sqlite_cursor.execute("SELECT * FROM txapelketa")
        data = sqlite_cursor.fetchall()

        # Datuak prozesatu
        for row in data:
            
            nan_value = row[0]
            denbora = row[3]
            puntuaketa = row[4]

            if check_nan_exists(postgres_cursor, nan_value):
                # Aktualizatu datuak
                update_nan_data(postgres_cursor, nan_value, denbora, puntuaketa)
            else:
                # Inserta egin
                insert_nan_data(postgres_cursor, row)

        # Commit egin datuak gordetzeko
        postgres_conn.commit()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # Konexioak itxi
        sqlite_conn.close()
        postgres_conn.close()

    return JSONResponse(content={"Mezua": "Datuak berritu dira."}, status_code=200)

# PostgreSQLko datuak lortu MVC barruan irakusteko
@app.get('/lortu_datuak')
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
        postgres_cursor.execute("SELECT id, izena, abizena, nan, puntuaketa, denbora FROM txapelketa_txapelketa")
        data = postgres_cursor.fetchall()

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # Konexioa itxi
        postgres_conn.close()

    return JSONResponse(content={"Jokalariak": data}, status_code=200)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8012)
