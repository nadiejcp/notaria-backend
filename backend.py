from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
import mysql.connector as connector
import uvicorn

app = FastAPI()

app.add_middleware(
    CORSMiddleware, 
    allow_origins=["https://www.plusnotary.net", "http://localhost:3000", "https://notaria-next-js.vercel.app"],
    allow_credentials=True, allow_methods=['*'], 
    allow_headers=['*'],
)

def get_db_connection(query, params, commit, one):
    conn = connector.connect(
        host='database-plus-notary.cj4g6ckwk9a3.us-east-2.rds.amazonaws.com',        # or your MySQL server IP
        user="admin",    # replace with your MySQL username
        password="mpdekAr8oqlSqzPNX4kX",# replace with your MySQL password
        database="database_notary",     # replace with your database name
        port=3306  # default MySQL port
    )
    cursor = conn.cursor(buffered=True)
    cursor.execute(query, params)
    answer = False
    if commit:
        conn.commit()
    elif one:
        answer= cursor.fetchone()
    else:
        answer = cursor.fetchall()
    cursor.close()
    conn.close()
    return answer

def retrieveInfo(data):
    return get_db_connection('SELECT nombres, apellidos, identificacion FROM users WHERE id = %s', (data,), False, True)

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
        record = get_db_connection('SELECT estado, codigo, fechaEmision, fechaCaducidad, enlaceDescarga, id FROM files WHERE codigo = %s', 
                                   (code,), False, True)
        if record is not None:
            files = get_db_connection('SELECT emisor_id, receptor_id FROM follows WHERE file_id = %s', (record[5],), False, False) 
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
                return {"message": dataDocument}        
    except connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {e}")
    raise HTTPException(status_code=404, detail="Archivo no encontrado")

@app.get("/userInfo/{id}")
async def getUserData(request: Request, id: str):
    if(await validateRequest(request)):
        record = get_db_connection('SELECT * FROM users WHERE identificacion = %s', (id,), False, True)
        if record:
            return {"message": record}
        return {"message": "Usuario no encontrado"}
    raise HTTPException(status_code=404, detail="Archivo no encontrado")

@app.get("/usersInfo")
async def getUsersData(request: Request):
    try:
        if(await validateRequest(request)):
            record =get_db_connection('SELECT nombres, apellidos, identificacion FROM users', (), False, False)
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
        record = get_db_connection('SELECT id FROM users WHERE identificacion = %s AND nacionalidad = %s', 
                                   (json_data.get('identificacion'), json_data.get("nacionalidad")), False, True)
        if (record is None):
            get_db_connection('INSERT INTO users (nombres, apellidos, fechaNacimiento, nacionalidad, identificacion) VALUES (%s, %s, %s, %s, %s)', 
                        (json_data.get('nombres'), json_data.get('apellidos'), json_data.get('fechaNacimiento'),
                            json_data.get('nacionalidad'), json_data.get('identificacion'),), True, False)
            return {"message": "Usuario guardado exitosamente"}
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
        record = get_db_connection('SELECT id FROM files WHERE codigo = %s', (json_data.get('code'),), False, True)
        if record is None:
            emisores = json_data.get('emisor').split(',')
            receptores = json_data.get('receptor').split(',')
            get_db_connection('INSERT INTO files (estado, codigo, fechaEmision, fechaCaducidad, enlaceDescarga) VALUES (%s, %s, %s, %s, %s) RETURNING id', 
                        (json_data.get('estado'), json_data.get('code'), json_data.get('fechaEmision'),
                            json_data.get('fechaCaducidad'), json_data.get('enlaceDescarga'),), True, False)
            for e in emisores:
                emisor = get_db_connection('SELECT id FROM users WHERE identificacion = %s', (e.strip(), ), False, True)
                if emisor is None:
                    raise HTTPException(status_code=403, detail=f"Emisor con identificacion {e.strip()} no existe en la base de datos")      
                for receptor in receptores:
                    receptorId = get_db_connection('SELECT id FROM users WHERE identificacion = %s', (receptor.strip(), ), False, True)
                    if receptorId is None:
                        raise HTTPException(status_code=403, detail=f"Receptor con identificacion {receptor.strip()} no existe en la base de datos") 
                    get_db_connection('INSERT INTO follows (emisor_id, receptor_id, file_id) VALUES (%s, %s, %s)',
                                   (emisor[0], receptorId[0], id,), True, False)            
            return {"message": "Archivo guardado exitosamente"}
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
        file = get_db_connection('SELECT id FROM files WHERE id = %s', (id, ), False, True) 
        if file is not None:       
            emisores = json_data.get('emisor').split(',')
            receptores = json_data.get('receptor').split(',')
            get_db_connection('DELETE FROM follows WHERE file_id = %s', (id,), True, False)
            get_db_connection("""UPDATE files SET estado = %s, codigo = %s, fechaEmision = %s, 
                           fechaCaducidad = %s, enlaceDescarga = %s WHERE id = %s""", 
                           (json_data.get('estado'), json_data.get('code'), json_data.get('fechaEmision'),
                            json_data.get('fechaCaducidad'), json_data.get('enlaceDescarga'), id, ), True, False)
            for e in emisores:
                emisor = get_db_connection('SELECT id FROM users WHERE identificacion = %s', (e.strip(), ), False, True)
                if emisor is None:
                    raise HTTPException(status_code=403, detail=f"Emisor con identificacion {e.strip()} no existe en la base de datos")      
                for receptor in receptores:
                    receptorId = get_db_connection('SELECT id FROM users WHERE identificacion = %s', (receptor.strip(), ), False, True)
                    if receptorId is None:
                        raise HTTPException(status_code=403, detail=f"Receptor con identificacion {receptor.strip()} no existe en la base de datos") 
                    get_db_connection('INSERT INTO follows (emisor_id, receptor_id, file_id) VALUES (%s, %s, %s)', 
                                      (emisor[0], receptorId[0], id,), True, False)    
            return {"message": "Archivo actualizado exitosamente"}
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
        record = get_db_connection('SELECT id FROM users WHERE id = %s', (id, ), False, True)
        if (record is not None):
            get_db_connection("""UPDATE users SET nombres = %s, apellidos = %s, fechaNacimiento = %s, 
                           nacionalidad = %s, identificacion = %s WHERE id = %s""", 
                           (json_data.get('nombres'), json_data.get('apellidos'), json_data.get('fechaNacimiento'),
                            json_data.get('nacionalidad'), json_data.get('identificacion'), id), True, False)
            return {"message": "Usuario actualizado exitosamente"}
        raise HTTPException(status_code=403, detail="Usuario no existe")        
    except connector.Error as e:
        raise HTTPException(status_code=500, detail=f"Error en la base de datos: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en el formato JSON: {e}")

host='127.0.0.1'
port=8000
uvicorn.run(app, host=host, port=port, reload=False)

