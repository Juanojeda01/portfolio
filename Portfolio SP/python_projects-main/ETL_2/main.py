# ETL con Pandas, Numpy, y datetime 

# Librerias 
import pandas as pd 
import datetime as dt
import numpy as np


# Rutas ventas xlsx
alkosto_file = 'Ventas/VENTAS ALKOSTO.xlsx'
exito_file = 'Ventas/EXITO.xlsx'
jumbo_files = ['Ventas/jumbo/ENERO.xlsx','Ventas/jumbo/FEBRERO.xlsx',
                        'Ventas/jumbo/MARZO.xlsx','Ventas/jumbo/ABRIL.xlsx',
                        'Ventas/jumbo/MAYO.xlsx','Ventas/jumbo/JUNIO.xlsx',
                        'Ventas/jumbo/JULIO.xlsx','Ventas/jumbo/AGOSTO.xlsx']
falabella_file ='Ventas/VENTAS FALABELLA.xlsx'

# Base Target

tg_file = 'TG/BASE TARGET.xlsx'

# Tablas de homologacion
refhom_file = 'Tablas de homologacion/Referencias Homolocion.xlsx'
sthom_hom = 'Tablas de homologacion/Almacen Homologados.xlsx'
tgh_file = 'Tablas de homologacion/TGH.xlsx'

# Base Inventario 

inv_file = 'BASE_INVENTARIO.xlsx'

def extract(alkosto_file, exito_file, jumbo_files, falabella_file, tg_file, refhom_file, sthom_hom, tgh_file, inv_file):
# Extraccion de archivos *xlsx

 # Dataframes 

    # Ventas 
        # Alkosto 
    df_alk = pd.read_excel(alkosto_file) 
        # Exito 
    df_ex = pd.concat(pd.read_excel(exito_file,sheet_name=None), ignore_index=True) 
        # Jumbo 
    df_jb = []
    for file in jumbo_files:  # Lista de jumbo
        df_jb.append(pd.read_excel(file))
    df_jb = pd.concat(df_jb,ignore_index=True) 
        # Falabella
    df1_fl = pd.read_excel(falabella_file,sheet_name='MONTO')  # Falabella 1
    df2_fl = pd.read_excel(falabella_file,sheet_name='UNIDADES')  # Falabella 2
        # Tablas homologadas
    df_rf = pd.read_excel(refhom_file) # Ref. Homologadas
    df_sh = pd.read_excel(sthom_hom) # St. Homologados
    df_m = pd.read_excel(tgh_file,sheet_name='MESES')# Meses
        # Target
    df_tg = pd.read_excel(tg_file,sheet_name='TG')
    df_tgh = pd.read_excel(tgh_file,sheet_name='TABLA HTG') # Tget Homologado

    df_ctg = pd.read_excel(tgh_file,sheet_name='ALMACENES TG') # Clientes target
        # Inventarios 
    df_inv = pd.read_excel(inv_file)

    return df_alk, df_jb, df_ex, df1_fl, df2_fl, df_rf, df_sh, df_m, df_tg, df_tgh, df_ctg, df_inv

def Transform(df_alk, df_jb, df_ex, df1_fl, df2_fl, df_rf, df_sh, df_m, df_tg, df_tgh, df_ctg, df_inv):
    #Formato VENTAS
        
            # Trns Falabella
                # Dataframes en blanco
    df3_fl = pd.DataFrame(columns=['PUNTO DE VENTA','EAN','MODELO','VALOR TOTAL','FECHA'])
    df4_fl = pd.DataFrame(columns=['EAN','UNIDADES'])
    for i in df1_fl.columns[3:]:  # Iteraciones por columna para llenar nuevos dataframes
        df = pd.DataFrame(df1_fl[df1_fl[i] != 0],columns= ['EAN','MODELO',i,'FECHA'])
        df.insert(0,'PUNTO DE VENTA',i)
        df3_fl = pd.concat([df3_fl,df.rename(columns={i:'VALOR TOTAL',})],ignore_index=True)
    for i in df2_fl.columns[3:]:
        df = pd.DataFrame(df2_fl[df2_fl[i] != 0],columns= ['EAN',i,'FECHA'])
        df4_fl = pd.concat([df4_fl,df.rename(columns={i:'UNIDADES'})],ignore_index=True)   
    df_fl = df3_fl.join(df4_fl['UNIDADES']) # Union y orden de columnas
    orden =['PUNTO DE VENTA','EAN','MODELO','UNIDADES','VALOR TOTAL','FECHA']
    df_fl = df_fl[orden]
            # Creacion del formato ventas
    df = pd.concat([df_alk,df_ex,df_jb,df_fl],ignore_index=True)
    df.insert(1,'TIPO2','SO')
    df = df.merge(df_rf)
    df = df.merge(df_sh,left_on='PUNTO DE VENTA',right_on='NOMBRE PDV').rename(columns=
                                             {'TIPO ALMACEN':'TIPO','CHANNEL':'CANAL',})
            # Mes y No.Semana 
    df['FECHA'] = pd.to_datetime(df['FECHA'],format='%m%d%y') 
    df['MES'] = df['FECHA'].dt.month  
    df['NUMERO SEMANA'] = df['FECHA'].dt.isocalendar().week
    
        # 'PROMOTER''REGIONAL LG' en blanco, base core store sin llave
    df.insert(1,'PROMOTER','')
    df.insert(1,'REGIONAL LG','')
    df.insert(1,'CORE STORE','')
    

            # Orden ventas
    orden_ventas = ['TIPO', 'TIPO2', 'CANAL',    'CADENA','SUBCADENA','PUNTO DE VENTA', 'HOMOLOGA ALMACEN','EAN','MODELO', 'REFERENCIA HOMOLOGADA',
    'CATEGORIA', 'SUBCATEGORIA', 'LINEA','SUBLINEA', 'UNIDADES', 'VALOR TOTAL','REGIONAL', 'CIUDAD' ,'NUMERO SEMANA', 'FECHA', 'MES TG', 'CORE STORE',
    'PROMOTER','REGIONAL LG', 'LINEA TG']
    
    form_ventas = df.merge(df_m,on='MES')
    form_ventas = form_ventas.drop('MES', axis=1)
    form_ventas = form_ventas.rename(columns={'MES NAME':'MES'})

    orden_ventas = ['TIPO', 'TIPO2', 'CANAL', 'CADENA',
    'SUBCADENA','PUNTO DE VENTA', 'HOMOLOGA ALMACEN',
    'EAN','MODELO', 'REFERENCIA HOMOLOGADA','CATEGORIA',
    'SUBCATEGORIA', 'LINEA','SUBLINEA', 'UNIDADES', 
    'VALOR TOTAL','REGIONAL', 'CIUDAD' ,'NUMERO SEMANA', 
    'FECHA', 'MES', 'CORE STORE','PROMOTER','REGIONAL LG', 
    'LINEA TG']

    form_ventas = form_ventas[orden_ventas]
#-----------------
    # Formato TG

    # Derretimiento por columna para llenar nuevos dataframe target
    pesos_tg = pd.melt(df_tg,id_vars=['CLIENTE','LINEA LG'],var_name='MES',ignore_index=True)

    # Agrupar por linea (Ventas)
    df = form_ventas.groupby(['LINEA TG', 'CADENA','HOMOLOGA ALMACEN','MES'])['UNIDADES'].mean().reset_index()

    form_tg = df.merge(df_tgh, on=['HOMOLOGA ALMACEN'])
    form_tg.insert(1,'TIPO2','TG')
    form_tg.insert(1,'REGIONAL LG','')
    form_tg.insert(1,'CORE STORE','')

    orden_tg = ['TIPO','TIPO2', 'CANAL', 'CADENA_x', 'SUBCADENA','HOMOLOGA ALMACEN','CATEGORIA', 'SUBCATEGORIA', 'LINEA','UNIDADES','REGIONAL', 'CIUDAD', 'MES', 'CORE STORE','REGIONAL LG',
    'LINEA TG_x']

    form_tg = form_tg[orden_tg]
    form_tg = form_tg.rename(columns=
                        {'CADENA_x':'CADENA',
                         'LINEA TG_x':'LINEA TG',})
    
#--------------
 # UNION PESOS Y FORMATO TG
    form_tg = form_tg.merge(df_ctg,on='CADENA')
    form_tg = form_tg.merge(pesos_tg,left_on=['CADENA TG','LINEA TG','MES'],right_on=['CLIENTE','LINEA LG','MES'])
    
    try:
        form_tg['UNIDADES'] = form_tg['UNIDADES'] / form_tg['value']
    except:
        np.inf
    form_tg['UNIDADES'] = form_tg['UNIDADES']
    orden_tg = ['TIPO','TIPO2', 'CANAL', 'CADENA', 'SUBCADENA','HOMOLOGA ALMACEN','CATEGORIA', 'SUBCATEGORIA', 'LINEA','UNIDADES','REGIONAL', 'CIUDAD','MES', 'CORE STORE','REGIONAL LG',
    'LINEA TG']
    form_tg = form_tg[orden_tg]
#------------
    # INVENTARIOS
    orden_inv = ['TIPO','TIPO2', 'CANAL', 'CADENA', 'SUBCADENA', 
    'PUNTO DE VENTA','HOMOLOGA ALMACEN','EAN','MODELO', 'REFERENCIA HOMOLOGADA', 'CATEGORIA', 'SUBCATEGORIA', 'LINEA','SUBLINEA','UNIDADES','VALOR TOTAL','REGIONAL','CIUDAD','NUMERO SEMANA', 'MES', 'CORE STORE','PROMOTER']
    form_inv = df_inv[orden_inv]

    return form_ventas, form_tg, form_inv


def load(form_ventas, form_tg, form_inv):
    form_ventas = form_ventas.to_excel('FORMATO VENTAS.xlsx', index=False)
    form_tg = form_tg.to_excel('FORMATO TG.xlsx',index=False)
    form_inv = form_inv.to_excel('FORMATO INVENTARIO.xlsx',index=False)

    return form_ventas, form_tg, form_inv


if __name__ == '__main__':
    df_alk, df_jb, df_ex, df1_fl, df2_fl, df_rf, df_sh, df_m, df_tg, df_tgh, df_ctg, df_inv= extract(alkosto_file, exito_file, jumbo_files, falabella_file, tg_file, refhom_file, sthom_hom, tgh_file, inv_file)
    form_ventas,form_tg, form_inv = Transform(df_alk, df_jb, df_ex, df1_fl, df2_fl, df_rf, df_sh, df_m, df_tg, df_tgh, df_ctg, df_inv)
    load(form_ventas, form_tg, form_inv)       



