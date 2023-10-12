# ETL with SQLORM and Numpy 
import sqlalchemy
import numpy as np
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import sessionmaker, relationship, declarative_base


# Engine db 
engine = sqlalchemy.create_engine("sqlite:///ventas_calzados.db")
base = sqlalchemy.orm.declarative_base()

# Class
class Ventas(base):
    __tablename__ = 'ventas'
    id = Column(Integer, primary_key=True)
    fecha = Column(String)
    producto_id = Column(Integer)
    pais = Column(String)
    genero = Column(String)
    talle = Column(String)
    precio = Column(String)

def create_schema():
    # Crear las tablas
    base.metadata.create_all(engine)
    
def read_db(): 
    # Session 
    Session = sessionmaker(bind=engine)
    session = Session()
    list_paises = []
    list_generos = []
    list_talles = []
    list_precios = []
    char_sin = 0
    # Querys 
    query_1 = session.query(Ventas.pais)
    query_1 = query_1.all()
    for row in query_1:
        row = str(row)
        char_sin = [x for x in row if x.isalpha() == True or x == " "]
        list_paises.append(("".join(char_sin)))
    query_2 = session.query(Ventas.genero)
    query_2 = query_2.all()
    for row in query_2:
        row = str(row)
        char_sin = [x for x in row if x.isalpha() == True ]
        list_generos.append(("".join(char_sin)))
    query_3 = session.query(Ventas.talle)
    query_3 = query_3.all()
    for row in query_3:
        row = str(row)
        char_sin = [x for x in row if x.isdigit() == True or x == "."]
        list_talles.append(("".join(char_sin)))
    query_4 = session.query(Ventas.precio)
    query_4 = query_4.all()
    for row in query_4:
        row = str(row)
        char_sin = [x for x in row if x.isdigit() == True or x == "."]
        list_precios.append(("".join(char_sin)))
    paises = np.array(list_paises)
    generos = np.array(list_generos)
    talles = np.array(list_talles,dtype=np.float64)
    precios = np.array(list_precios,dtype=np.float64)
    return paises, generos, talles, precios

def obtener_paises_unicos(paises):
    paises_unicos = np.unique(paises)
    return paises_unicos

def obtener_ventas_por_pais(paises_objetivo, paises, precios):
    dicc = {}
    for x in paises_objetivo:
        sum = 0 
        if x == "Germany":
            mask = paises == "Germany"
            data = precios[mask]
            for x in data:
                sum += x
            dicc["Germany"] = sum
        elif x == "United Kingdom":
            mask = paises == "United Kingdom"
            data = precios[mask]
            for x in data:
                sum += x
            dicc["United Kingdom"] = sum
        elif x == "Canada":
            mask = paises == "Canada"
            data = precios[mask]
            for x in data:
                sum += x
            dicc["Canada"] = sum
        elif x == "United States":
            mask = paises == "United States"
            data = precios[mask]
            for x in data:
                sum += x
            dicc["United States"] = sum
    return dicc

def obtener_calzado_mas_vendido_por_pais(paises_objetivo, paises, talles):
    dicc = {}
    dicc = {}
    for x in paises_objetivo:
        if x == "Germany":
            mask = paises == "Germany"
            tallas, cuenta= np.unique(talles[mask],return_counts=True)
            tmax= tallas[cuenta.argmax()]
            dicc["Germany"] = tmax
        elif x == "United Kingdom":
            mask = paises == "United Kingdom"
            tallas, cuenta= np.unique(talles[mask],return_counts=True)
            tmax= tallas[cuenta.argmax()]
            dicc["United Kingdom"] = tmax
        elif x == "Canada":
            mask = paises == "Canada"
            tallas, cuenta= np.unique(talles[mask],return_counts=True)
            tmax= tallas[cuenta.argmax()]
            dicc["Canada"] = tmax
        elif x == "United States":
            mask = paises == "United States"
            tallas, cuenta= np.unique(talles[mask],return_counts=True)
            tmax= tallas[cuenta.argmax()]
            dicc["United States"] = tmax
    return dicc

def obtener_ventas_por_genero_pais(paises_objetivo, genero_objetivo, paises, generos):
    dicc = {}
    for x in paises_objetivo:
        count = 0
        if x == "Germany":
            if genero_objetivo == "Female":
                mask = (generos == "Female") & (paises == "Germany")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["Germany"] = count[0]
            elif genero_objetivo == "Male":
                mask = (generos == "Male") & (paises == "Germany")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["Germany"] = count[0]
            elif genero_objetivo == "Unix":
                mask = (generos == "Unix") & (paises == "Germany")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["Germany"] = count[0]
        if x == "United States":
            if genero_objetivo == "Female":
                mask = (generos == "Female") & (paises == "United States")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["United States"] = count[0]
            elif genero_objetivo == "Male":
                mask = (generos == "Male") & (paises == "United States")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["United States"] = count[0]
            elif genero_objetivo == "Unix":
                mask = (generos == "Unix") & (paises == "United States")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["United States"] = count[0]
        if x == "United Kingdom":
            if genero_objetivo == "Female":
                mask = (generos == "Female") & (paises == "United Kingdom")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["United Kingdom"] = count[0]
            elif genero_objetivo == "Male":
                mask = (generos == "Male") & (paises == "United Kingdom")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["United Kingdom"] = count[0]
            elif genero_objetivo == "Unix":
                mask = (generos == "Unix") & (paises == "United Kingdom")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["United Kingdom"] = count[0]    
        if x == "Canada":
            if genero_objetivo == "Female":
                mask = (generos == "Female") & (paises == "Canada")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["Canada"] = count[0]
            elif genero_objetivo == "Male":
                mask = (generos == "Male") & (paises == "Canada")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["Canada"] = count[0]
            elif genero_objetivo == "Unix":
                mask = (generos == "Unix") & (paises == "Canada")
                data,count = np.unique(paises[mask],return_counts=True)
                dicc["Canada"] = count[0]         
    return dicc

if __name__ == "__main__":
    paises_objetivo = ["Germany","United States","Canada","United Kingdom"]
    genero_objetivo = "Female"
    paises, generos, talles, precios = read_db()
    print(obtener_paises_unicos(paises))
    print(obtener_ventas_por_pais(paises_objetivo, paises, precios))
    print(obtener_calzado_mas_vendido_por_pais(paises_objetivo, paises, talles))
    print(obtener_ventas_por_genero_pais(paises_objetivo, genero_objetivo, paises, generos))



    

     
