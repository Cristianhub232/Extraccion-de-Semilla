import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from io import StringIO

def create_conn():
    try:
        conn = psycopg2.connect(
            host="172.16.132.64",
            database="pentaho",
            user="ifiscalfe",
            password="ifiscalfe",
            port="5432"
        )
        return conn
    except OperationalError as e:
        print(f"Error de conexión: {e}")
        return None

csv_path = 'C:/Users/cgbrito/Desktop/test/archivopivote.csv'
df = pd.read_csv(csv_path, encoding='utf-8', sep=',', dtype={'Num Planilla': str})
print(df.columns.tolist())

# Limpia la columna 'Num Planilla'
df['Num Planilla'] = df['Num Planilla'].str.replace(',', '', regex=False)

conn = create_conn()
if conn:
    try:
        with conn.cursor() as cur:
            # 1. Crea una tabla temporal
            cur.execute("""
                CREATE TEMP TABLE temp_csv_data (
                    rif VARCHAR(20),
                    planilla VARCHAR(30)
                ) ON COMMIT DROP;
            """)
            # NO HAGAS conn.commit() AQUÍ

            # 2. Copia los datos del DataFrame a la tabla temporal
            csv_buffer = StringIO()
            df[['Rif Contribuyente', 'Num Planilla']].to_csv(csv_buffer, sep='\t', header=False, index=False)
            csv_buffer.seek(0)
            cur.copy_from(csv_buffer, 'temp_csv_data', columns=('rif', 'planilla'), sep='\t')
            # NO HAGAS conn.commit() AQUÍ

            # 3. Ejecuta la consulta optimizada usando JOIN
            query = """
                SELECT 
                    t.rif_contribuyente_transaccion, 
                    t.fecha_documento_transaccion, 
                    t.numero_documento_transaccion, 
                    t.descripcion_transaccion, 
                    t.fecha_registro_transaccion, 
                    t.numero_expediente_transaccion, 
                    t.monto_transaccion
                FROM dbo.transaccion t
                JOIN temp_csv_data tmp
                  ON t.rif_contribuyente_transaccion = tmp.rif
                 AND t.numero_documento_transaccion = tmp.planilla
            """
            cur.execute(query)
            resultados = cur.fetchall()

            # 4. Obtén los nombres de las columnas
            column_names = [desc[0] for desc in cur.description]

        # 5. Guarda los resultados en CSV
        if resultados:
            df_resultados = pd.DataFrame(resultados, columns=column_names)
            df_resultados.to_csv('resultados_transaccion.csv', index=False, encoding='utf-8')
            print('Archivo CSV generado: resultados_transaccion.csv')
        else:
            print('No se encontraron resultados para guardar.')

    except Exception as e:
        print(f"Error durante la ejecución: {e}")
    finally:
        conn.close()
else:
    print('No se pudo establecer conexión con la base de datos.')
