import datetime
from sqlalchemy import create_engine
import databases
import sqlalchemy
from fastapi import FastAPI
from pydantic import BaseModel
from sqlalchemy.orm import sessionmaker, declarative_base
from typing import List

DB_USER = "postgres"
DB_NAME = "test"
DB_PASSWORD = "Vrt342zf"
DB_HOST = "127.0.0.1"


#Параметры подключения к Postgresql
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"
print(DATABASE_URL)
database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

#  postgres
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Создание таблиц
tovar = sqlalchemy.Table(
    "tovar",
    metadata,
    sqlalchemy.Column("id_articul", sqlalchemy.Integer, primary_key = True, autoincrement = True, unique = True),
    sqlalchemy.Column("naimenovanie", sqlalchemy.String),
    sqlalchemy.Column("price", sqlalchemy.Integer),
    sqlalchemy.Column("kolich_sklad", sqlalchemy.Integer),
    )

magazin = sqlalchemy.Table(
    "magazin",
    metadata,
    sqlalchemy.Column("id_magazin", sqlalchemy.Integer, primary_key = True, autoincrement = True, unique = True),
    sqlalchemy.Column("adress", sqlalchemy.String),
    )


prodazhi = sqlalchemy.Table(
    "prodazhi",
    metadata,
    sqlalchemy.Column("id_prod", sqlalchemy.Integer, primary_key = True, autoincrement = True, unique = True),
    sqlalchemy.Column("id_tovar", sqlalchemy.Integer),
    sqlalchemy.Column("naimenovanie_kod", sqlalchemy.String),
    sqlalchemy.Column("price", sqlalchemy.Integer),
    sqlalchemy.Column("kolich_prodan", sqlalchemy.Integer),
    sqlalchemy.Column("magazin_prodavshi_id", sqlalchemy.Integer),
    sqlalchemy.Column("viruchka_sum", sqlalchemy.Integer),
    sqlalchemy.Column("data_prodazhi", sqlalchemy.DateTime),
    )



metadata.create_all(engine)

#Наполнение таблиц БД

ins_mag_query = magazin.insert().values(id_magazin = '1', adress = 'Krasnodar, mira 5')
conn = engine.connect()
r = conn.execute(ins_mag_query)
r = conn.execute(ins_mag_query, [
        {
            "id_magazin": "2",
            "adress": "Volgograd Pr.revolut 7"
        },
        {
            "id_magazin": "3",
            "adress": "Voronezh, Ciolkovskogo 4"
        },
        {
            "id_magazin": "4",
            "adress": "Nalchik, Lenina 50"
        },
        {
            "id_magazin": "5",
            "adress": "Kazan, Kosmicheskay 25"
        },
        {
            "id_magazin": "6",
            "adress": "Voronezh, Montazhnikov 8"
        },
        {
            "id_magazin": "7",
            "adress": "na.Chelny, Lenina 17"
        },
        {
            "id_magazin": "8",
            "adress": "Vologda Otlichnikov 17"
        },
        {
            "id_magazin": "9",
            "adress": "Ekaterinburg, Visaotnay 43"
        },
        {
            "id_magazin": "10",
            "adress": "Perm, Lenina 30"
        },
       {
            "id_magazin": "11",
            "adress": "Orenburg, Lomonosova 27"
        },
        {
            "id_magazin": "12",
            "adress": "Kursk, Parkovaya 28"
        },
        {
            "id_magazin": "13",
            "adress": "Tambov, Lenina 13"
        },
       {
            "id_magazin": "14",
            "adress": "Belgorod, Paromnaya 24"
        },
        {
            "id_magazin": "15",
            "adress": "Ryzan, Lenina 15"
        },
    ])



ins_prod_query = prodazhi.insert().values(id_prod='1', id_tovar='3', naimenovanie_kod='Smetana',
                                          price='30', kolich_prodan='4',
                                          magazin_prodavshi_id='7',viruchka_sum= '120',
                                          data_prodazhi='2023-01-25 22:23')
connes = engine.connect()
f = connes.execute(ins_prod_query)
f = connes.execute(ins_prod_query, [
        {
            "id_prod": "2",
            "id_tovar": "5",
            "naimenovanie_kod": "Tomat",
            "price": "130",
            "kolich_prodan": "14",
            "magazin_prodavshi_id": "7",
            "viruchka_sum": "910",
            "data_prodazhi": "2023-01-23 21:27"
        },
        {
             "id_prod": "3",
             "id_tovar": "7",
             "naimenovanie_kod": "Smetana",
             "price": "550",
             "kolich_prodan": "17",
             "magazin_prodavshi_id": "3",
             "viruchka_sum": "7500",
             "data_prodazhi": "2023-01-15 12:24"
        },
        {
             "id_prod": "4",
             "id_tovar": "4",
             "naimenovanie_kod": "Cheese",
             "price": "350",
             "kolich_prodan": "11",
             "magazin_prodavshi_id": "11",
             "viruchka_sum": "3700",
             "data_prodazhi": "2023-01-14 15:24"
        },
        {
             "id_prod": "5",
             "id_tovar": "3",
             "naimenovanie_kod": "Tomat",
             "price": "80",
             "kolich_prodan": "21",
             "magazin_prodavshi_id": "13",
             "viruchka_sum": "1700",
             "data_prodazhi": "2023-01-01 21:23"
        },
        {
             "id_prod": "6",
             "id_tovar": "9",
             "naimenovanie_kod": "Sok",
             "price": "80",
             "kolich_prodan": "14",
             "magazin_prodavshi_id": "4",
             "viruchka_sum": "1200",
             "data_prodazhi": "2023-01-02 18:15"
        },
        {
             "id_prod": "7",
             "id_tovar": "11",
             "naimenovanie_kod": "Kartofel",
             "price": "35",
             "kolich_prodan": "37",
             "magazin_prodavshi_id": "4",
             "viruchka_sum": "1050",
             "data_prodazhi": "2023-01-04 10:25"
        },
        {
             "id_prod": "8",
             "id_tovar": "15",
             "naimenovanie_kod": "Chokolad",
             "price": "150",
             "kolich_prodan": "20",
             "magazin_prodavshi_id": "7",
             "viruchka_sum": "300",
             "data_prodazhi": "2023-01-01 09:27"
        },
        {
             "id_prod": "9",
             "id_tovar": "7",
             "naimenovanie_kod": "Govyadina",
             "price": "80",
             "kolich_prodan": "17",
             "magazin_prodavshi_id": "7",
             "viruchka_sum": "1360",
             "data_prodazhi": "2023-01-05 11:23"
        },
        {
             "id_prod": "10",
             "id_tovar": "3",
             "naimenovanie_kod": "Smetana",
             "price": "230",
             "kolich_prodan": "23",
             "magazin_prodavshi_id": "9",
             "viruchka_sum": "4670",
             "data_prodazhi": "2023-01-07 22:24"
        },
        {
             "id_prod": "11",
             "id_tovar": "5",
             "naimenovanie_kod": "Tomat",
             "price": "130",
             "kolich_prodan": "14",
             "magazin_prodavshi_id": "4",
             "viruchka_sum": "1700",
             "data_prodazhi": "2023-01-07 21:53"
        },
        {
             "id_prod": "12",
             "id_tovar": "2",
             "naimenovanie_kod": "Moloko",
             "price": "70",
             "kolich_prodan": "27",
             "magazin_prodavshi_id": "7",
             "viruchka_sum": "1490",
             "data_prodazhi": "2023-01-08 23:23"
        },
        {
             "id_prod": "13",
             "id_tovar": "7",
             "naimenovanie_kod": "Govyadina",
             "price": "550",
             "kolich_prodan": "17",
             "magazin_prodavshi_id": "11",
             "viruchka_sum": "8550",
             "data_prodazhi": "2023-01-03 15:43"
        },
        {
             "id_prod": "14",
             "id_tovar": "8",
             "naimenovanie_kod": "Gribi",
             "price": "150",
             "kolich_prodan": "18",
             "magazin_prodavshi_id": "7",
             "viruchka_sum": "2700",
             "data_prodazhi": "2023-01-03 14:13"
        },
        {
             "id_prod": "15",
             "id_tovar": "5",
             "naimenovanie_kod": "Tomat",
             "price": "130",
             "kolich_prodan": "14",
             "magazin_prodavshi_id": "3",
             "viruchka_sum": "1950",
             "data_prodazhi": "2023-01-02 17:23"
        },
        {
             "id_prod": "16",
             "id_tovar": "1",
             "naimenovanie_kod": "Hleb",
             "price": "30",
             "kolich_prodan": "75",
             "magazin_prodavshi_id": "5",
             "viruchka_sum": "2250",
             "data_prodazhi": "2023-01-04 15:28"
        },
        {
             "id_prod": "17",
             "id_tovar": "4",
             "naimenovanie_kod": "Cheese",
             "price": "350",
             "kolich_prodan": "38",
             "magazin_prodavshi_id": "4",
             "viruchka_sum": "6450",
             "data_prodazhi": "2023-01-08 19:17"
        },
        {
             "id_prod": "18",
             "id_tovar": "2",
             "naimenovanie_kod": "Moloko",
             "price": "70",
             "kolich_prodan": "63",
             "magazin_prodavshi_id": "6",
             "viruchka_sum": "4410",
             "data_prodazhi": "2023-01-05 15:16"
        },
        {
             "id_prod": "19",
             "id_tovar": "12",
             "naimenovanie_kod": "Banan",
             "price": "75",
             "kolich_prodan": "44",
             "magazin_prodavshi_id": "3",
             "viruchka_sum": "3300",
             "data_prodazhi": "2023-01-11 14:43"
        },
        {
            "id_prod": "20",
            "id_tovar": "5",
            "naimenovanie_kod": "Tomat",
            "price": "130",
            "kolich_prodan": "14",
            "magazin_prodavshi_id": "7",
            "viruchka_sum": "1900",
            "data_prodazhi": "2023-01-15 07:13"
        },
        {
             "id_prod": "21",
             "id_tovar": "3",
             "naimenovanie_kod": "Smetana",
             "price": "80",
             "kolich_prodan": "21",
             "magazin_prodavshi_id": "8",
             "viruchka_sum": "1680",
             "data_prodazhi": "2023-01-17 23:27"
        },
        {
             "id_prod": "22",
             "id_tovar": "7",
             "naimenovanie_kod": "Govyadinaa",
             "price": "550",
             "kolich_prodan": "47",
             "magazin_prodavshi_id": "7",
             "viruchka_sum": "25850",
             "data_prodazhi": "2023-01-18 15:28"
        },
        {
             "id_prod": "23",
             "id_tovar": "6",
             "naimenovanie_kod": "Svinin",
             "price": "400",
             "kolich_prodan": "50",
             "magazin_prodavshi_id": "9",
             "viruchka_sum": "2000",
             "data_prodazhi": "2023-01-24 11:23"
        },
        {
             "id_prod": "24",
             "id_tovar": "3",
             "naimenovanie_kod": "Smetana",
             "price": "80",
             "kolich_prodan": "21",
             "magazin_prodavshi_id": "3",
             "viruchka_sum": "1680",
             "data_prodazhi": "2023-01-08 14:53"
        },
        {
             "id_prod": "25",
             "id_tovar": "5",
             "naimenovanie_kod": "Tomat",
             "price": "130",
             "kolich_prodan": "15",
             "magazin_prodavshi_id": "2",
             "viruchka_sum": "1950",
             "data_prodazhi": "2023-01-13 12:23"
        },
        {
             "id_prod": "26",
             "id_tovar": "7",
             "naimenovanie_kod": "Govyadina",
             "price": "550",
             "kolich_prodan": "17",
             "magazin_prodavshi_id": "5",
             "viruchka_sum": "8500",
             "data_prodazhi": "2023-01-15 23:45"
        },
        {
             "id_prod": "27",
             "id_tovar": "3",
             "naimenovanie_kod": "Smetana",
             "price": "80",
             "kolich_prodan": "21",
             "magazin_prodavshi_id": "7",
             "viruchka_sum": "1680",
             "data_prodazhi": "2023-01-14 23:13"
        },
        {
             "id_prod": "28",
             "id_tovar": "5",
             "naimenovanie_kod": "Tomat",
             "price": "130",
             "kolich_prodan": "14",
             "magazin_prodavshi_id": "7",
             "viruchka_sum": "1850",
             "data_prodazhi": "2023-01-08 22:25"
        },
       {
             "id_prod": "29",
             "id_tovar": "3",
             "naimenovanie_kod": "Smetana",
             "price": "80",
             "kolich_prodan": "21",
             "magazin_prodavshi_id": "7",
             "viruchka_sum": "1680",
             "data_prodazhi": "2023-01-24 10:25"
        },
        {
             "id_prod": "30",
             "id_tovar": "5",
             "naimenovanie_kod": "Tomat",
             "price": "130",
             "kolich_prodan": "14",
             "magazin_prodavshi_id": "7",
             "viruchka_sum": "1800",
             "data_prodazhi": "2023-01-23 15:17"
        },
    ])


ins_tov_query = tovar.insert().values(id_articul = '1', naimenovanie = 'Hleb', price = '30', kolich_sklad = '500')
conn = engine.connect()
t = conn.execute(ins_tov_query)
t = conn.execute(ins_tov_query, [
        {
            "id_articul": "2",
            "naimenovanie": "Moloko",
            "price": "70",
            "kolich_sklad": "100",
        },
        {
            "id_articul": "3",
            "naimenovanie": "Smetana",
            "price": "80",
            "kolich_sklad": "85",
        },
        {
            "id_articul": "4",
            "naimenovanie": "Cheese",
            "price": "350",
            "kolich_sklad": "50",
        },
        {
            "id_articul": "5",
            "naimenovanie": "Tomat",
            "price": "130",
            "kolich_sklad": "30",
        },
        {
            "id_articul": "6",
            "naimenovanie": "Svinina",
            "price": "400",
            "kolich_sklad": "45",
        },
        {
            "id_articul": "7",
            "naimenovanie": "Govyadina",
            "price": "550",
            "kolich_sklad": "35",
        },
       {
            "id_articul": "8",
            "naimenovanie": "Gribi",
            "price": "150",
            "kolich_sklad": "70",
        },
        {
            "id_articul": "9",
            "naimenovanie": "Sok",
            "price": "80",
            "kolich_sklad": "140",
        },
        {
            "id_articul": "10",
            "naimenovanie": "Riba",
            "price": "230",
            "kolich_sklad": "55",
        },
       {
            "id_articul": "11",
            "naimenovanie": "Kartofel",
            "price": "35",
            "kolich_sklad": "400",
        },
        {
            "id_articul": "12",
            "naimenovanie": "Banan",
            "price": "75",
            "kolich_sklad": "70",
        },
        {
            "id_articul": "13",
            "naimenovanie": "Krevetki",
            "price": "550",
            "kolich_sklad": "35",
        },
       {
            "id_articul": "14",
            "naimenovanie": "Konfeti",
            "price": "230",
            "kolich_sklad": "170",
        },
        {
            "id_articul": "15",
            "naimenovanie": "Chokolad",
            "price": "100",
            "kolich_sklad": "130",
        },
    ])


#engine.execute(ins_mag_query)

class ProdIn(BaseModel):
    id_prod = 31
    id_tovar = 3
    naimenovanie_kod = "Smetana"
    price = 80
    kolich_prodan = 1
    magazin_prodavshi_id = 5
    viruchka_sum = 80
    data_prodazhi = datetime.datetime.now()


class Prod(BaseModel):
    #id = 55
    id_prod = 3
    id_tovar = 3
    naimenovanie_kod = "Smetana"
    price = 80
    kolich_prodan = 1
    magazin_prodavshi_id = 5
    viruchka_sum = 80
    data_prodazhi = datetime.datetime.now()


app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

@app.get("/magazin/")
async def read_magazin():
    query = magazin.select()
    return await database.fetch_all(query)

@app.get("/tovar/")
async def read_tovar():
    query = tovar.select()
    return await database.fetch_all(query)

@app.post("/prodazhi/", response_model=Prod)
async def create_prod(prod_i: ProdIn):
    query = prodazhi.insert().values(id_prod = prod_i.id_prod, id_tovar = prod_i.id_tovar,
                                     naimenovanie_kod = prod_i.naimenovanie_kod, price = prod_i.price,
                                     kolich_prodan = prod_i.kolich_prodan, magazin_prodavshi_id = prod_i.magazin_prodavshi_id,
                                     viruchka_sum = prod_i.viruchka_sum, data_prodazhi= prod_i.data_prodazhi)
    last_record_id = await database.execute(query)
    return  {"id": last_record_id} #("Запись по продаже в БД сделана")


@app.get("/tovar_top10/")
async def read_tovar_10():
    query = f"select prodazhi.id_tovar, prodazhi.naimenovanie_kod, prodazhi.kolich_prodan" \
            f" from prodazhi where prodazhi.data_prodazhi >= (now() - interval '31 day')" \
            f" group by id_prod, naimenovanie_kod order by kolich_prodan desc limit 10"
    return await database.fetch_all(query)


@app.get("/magazin_top10/")
async def read_magaz_10():
    query = f"select magazin.id_magazin, magazin.adress, prodazhi.viruchka_sum" \
            f" from prodazhi, magazin where prodazhi.data_prodazhi >= (now() - interval '31 day')" \
            f" order by viruchka_sum DESC LIMIT 10;"
    return await database.fetch_all(query)
#query = f"select magazin.id_magazin, magazin.adress, prodazhi.viruchka_sum from prodazhi, magazin where prodazhi.data_prodazhi >= (now() - interval '31 day') group by id_magazin, id_prod, naimenovanie_kod, viruchka_sum order by viruchka_sum desc limit 10"