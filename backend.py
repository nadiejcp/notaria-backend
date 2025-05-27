from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector as connector
import uvicorn

app = FastAPI()

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["https://www.plusnotary.net", "http://localhost:3000"],
    allow_credentials=True, allow_methods=['*'], 
    allow_headers=['*'],
)

def get_db_connection():
    conn = connector.connect(
        host='database-plus-notary.cj4g6ckwk9a3.us-east-2.rds.amazonaws.com',        # or your MySQL server IP
        user="admin",    # replace with your MySQL username
        password="mpdekAr8oqlSqzPNX4kX",# replace with your MySQL password
        database="database_notary",     # replace with your database name
        port=3306  # default MySQL port
    )
    return conn

def close(cursor, conn):
    if cursor is not None:
        cursor.close()
    if conn is not None:
        conn.close()

def retrieveInfo(data):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('SELECT nombres, apellidos, identificacion FROM users WHERE id = ?', (data,)) 
    info = cursor.fetchone()
    close(cursor, conn)
    return info

def substractInfo(emisores):
    emisores_data = [" ".join(emisores[0][:2])]
    emisores_cedula = [emisores[0][2]]
    for n in range(1, len(emisores)):
        emisor = " ".join(emisores[n][:2])
        if emisor not in emisores_data:
            emisores_data.append(" ".join(emisores[n][:2]))
        if emisores[n][2] not in emisores_cedula:
            emisores_cedula.append(emisores[n][2])
    return ' - '.join(emisores_data), ' - '.join(emisores_cedula)

async def validateRequest(request: Request):
    auth_username = request.headers.get('username')
    auth_password = request.headers.get('password')
    if auth_password == "9plus*-*notary9" and auth_username == "luisa":
        if request.method == "POST":
            json_data = await request.json()
            if json_data.get('origin') == 'plusServices':                
                return json_data
        return True
    return False
             
@app.get("/licencia/{code}")
def getFileData(code: str):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT estado, codigo, fechaEmision, fechaCaducidad, enlaceDescarga, id FROM files WHERE codigo = ?', (code,)) 
        record = cursor.fetchone()
        if record is not None:
            cursor.execute('SELECT emisor_id, receptor_id FROM follows WHERE file_id = ?', (record[5],)) 
            files = cursor.fetchall()
            emisores = []
            receptores = []
            for file in files:
                file = list(file)
                emisores.append(retrieveInfo(file[0]))
                receptores.append(retrieveInfo(file[1]))
            if (len(emisores) > 0) and (len(receptores) > 0):
                emisores_data, emisores_cedula = substractInfo(emisores)
                receptores_data, receptores_cedula = substractInfo(receptores)
                dataDocument = [record[0], record[1], emisores_data, emisores_cedula,
                        receptores_data, receptores_cedula, record[2], record[3], record[4], record[5]]
                close(cursor, conn)
                return {"message": dataDocument}        
    except connector.Error as e:
        close(cursor, conn)
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {e}")
    close(cursor, conn)
    raise HTTPException(status_code=404, detail="Archivo no encontrado")

@app.get("/userInfo/{id}")
async def getUserData(request: Request, id: str):
    if(await validateRequest(request)):
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM users WHERE identificacion = ?', (id,)) 
        record = cursor.fetchone()
        if record:
            close(cursor, conn)
            return {"message": record}
        close(cursor, conn)
        return {"message": "Usuario no encontrado"}
    raise HTTPException(status_code=404, detail="Archivo no encontrado")

@app.get("/usersInfo")
async def getUsersData(request: Request):
    try:
        if(await validateRequest(request)):
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute('SELECT nombres, apellidos, identificacion FROM users') 
            record = cursor.fetchall()
            close(cursor, conn)
            return {"users": record}
        raise HTTPException(status_code=404, detail=f"You are being tracked")
    except connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en el formato JSON: {e}")

@app.post("/saveInfoUsuario")
async def saveInfoUsuario(request: Request):
    try:
        json_data = await validateRequest(request)
        if(not json_data):
            raise HTTPException(status_code=404, detail=f"You are being tracked")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f'SELECT id FROM users WHERE identificacion = {json_data.get('identificacion')} AND nacionalidad = {json_data.get("nacionalidad")}')
        if (cursor.fetchone() is None):
            cursor.execute('INSERT INTO users (nombres, apellidos, fechaNacimiento, nacionalidad, identificacion) VALUES (?, ?, ?, ?, ?)', 
                        (json_data.get('nombres'), json_data.get('apellidos'), json_data.get('fechaNacimiento'),
                            json_data.get('nacionalidad'), json_data.get('identificacion'),))
            conn.commit()
            close(cursor, conn)
            return {"message": "Usuario guardado exitosamente"}
        close(cursor, conn)
        raise HTTPException(status_code=403, detail="Usuario ya existe")        
    except connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en el formato JSON: {e}")
    
@app.post("/saveInfoDocumento")
async def saveInfoFile(request: Request):
    try:
        json_data = await validateRequest(request)
        if (not json_data):
            raise HTTPException(status_code=404, detail=f"You are being tracked")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM files WHERE codigo = ?', (json_data.get('code'),))
        record = cursor.fetchone()
        if record is None:
            emisores = json_data.get('emisor').split(',')
            receptores = json_data.get('receptor').split(',')
            cursor.execute('INSERT INTO files (estado, codigo, fechaEmision, fechaCaducidad, enlaceDescarga) VALUES (?, ?, ?, ?, ?) RETURNING id', 
                        (json_data.get('estado'), json_data.get('code'), json_data.get('fechaEmision'),
                            json_data.get('fechaCaducidad'), json_data.get('enlaceDescarga'),))
            id = cursor.fetchone()[0]
            for e in emisores:
                cursor.execute(f'SELECT id FROM users WHERE identificacion = ?', (e.strip(), ))
                emisor = cursor.fetchone()
                if emisor is None:
                    raise HTTPException(status_code=403, detail=f"Emisor con identificacion {e.strip()} no existe en la base de datos")      
                for receptor in receptores:
                    cursor.execute(f'SELECT id FROM users WHERE identificacion = ?', (receptor.strip(), ))
                    receptorId = cursor.fetchone()
                    if receptorId is None:
                        raise HTTPException(status_code=403, detail=f"Receptor con identificacion {receptor.strip()} no existe en la base de datos") 
                    cursor.execute('INSERT INTO follows (emisor_id, receptor_id, file_id) VALUES (?, ?, ?)',
                                   (emisor[0], receptorId[0], id,))            
            conn.commit()
            close(cursor, conn)
            return {"message": "Archivo guardado exitosamente"}
        close(cursor, conn)
        raise HTTPException(status_code=403, detail="Archivo ya existe")
    except connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en el formato JSON: {e}")

@app.post("/updateInfoDocumento/{id}")
async def updateInfoFile(request: Request, id: str):
    try:
        json_data = await validateRequest(request)
        if (not json_data):
            raise HTTPException(status_code=404, detail=f"You are being tracked")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(f'SELECT id FROM files WHERE id = ?', (id, )) 
        if cursor.fetchone() is not None:       
            emisores = json_data.get('emisor').split(',')
            receptores = json_data.get('receptor').split(',')
            cursor.execute("""UPDATE files SET estado = ?, codigo = ?, fechaEmision = ?, 
                           fechaCaducidad = ?, enlaceDescarga = ? WHERE id = ?""", 
                           (json_data.get('estado'), json_data.get('code'), json_data.get('fechaEmision'),
                            json_data.get('fechaCaducidad'), json_data.get('enlaceDescarga'), id, ))
            for e in emisores:
                cursor.execute(f'SELECT id FROM users WHERE identificacion = ?', (e.strip(), ))
                emisor = cursor.fetchone()
                if emisor is None:
                    raise HTTPException(status_code=403, detail=f"Emisor con identificacion {e.strip()} no existe en la base de datos")      
                for receptor in receptores:
                    cursor.execute(f'SELECT id FROM users WHERE identificacion = ?', (receptor.strip(), ))
                    receptorId = cursor.fetchone()
                    if receptorId is None:
                        raise HTTPException(status_code=403, detail=f"Receptor con identificacion {receptor.strip()} no existe en la base de datos") 
                    cursor.execute('UPDATE follows SET emisor_id = ?, receptor_id = ? WHERE file_id = ?',
                                   (emisor[0], receptorId[0], id,))         
            conn.commit()
            close(cursor, conn)
            return {"message": "Archivo actualizado exitosamente"}
        close(cursor, conn)
        raise HTTPException(status_code=403, detail="Archivo no existe")
    except connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en el formato JSON: {e}")

@app.post("/updateInfoUsuario/{id}")
async def updateInfoUsuario(request: Request, id: str):
    try:
        json_data = await validateRequest(request)
        if(not json_data):
            raise HTTPException(status_code=404, detail=f"You are being tracked")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM users WHERE id = ?', (id, ))
        record = cursor.fetchone()
        if (record is not None):
            cursor.execute("""UPDATE users SET nombres = ?, apellidos = ?, fechaNacimiento = ?, 
                           nacionalidad = ?, identificacion = ? WHERE id = ?""", 
                           (json_data.get('nombres'), json_data.get('apellidos'), json_data.get('fechaNacimiento'),
                            json_data.get('nacionalidad'), json_data.get('identificacion'), id))
            conn.commit()
            close(cursor, conn)
            return {"message": "Usuario actualizado exitosamente"}
        close(cursor, conn)
        raise HTTPException(status_code=403, detail="Usuario no existe")        
    except connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en el formato JSON: {e}")

host='127.0.0.1'
port=8000
uvicorn.run(app, host=host, port=port, reload=False)

