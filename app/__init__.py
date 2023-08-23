from flask import Flask,request,jsonify
import pyodbc
from waitress import serve

import json
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

server = "Jorgeserver.database.windows.net"
database = 'DPL' 
username = 'Jmmc' 
password = 'ChaosSoldier01'  
cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)




@app.route('/')
#python .\src\app.py
def RespuestaPost():
    import pandas as pd

    cursor = cnxn.cursor()
    queryIEES = "SELECT * FROM [citas].[PRTAL_Transportistas]"
    queryRegistroPenal = "SELECT * FROM [citas].[RegistroPenal]"
    queryRegistroSeguridad = "Select cedula as Cedula,estado as Examen_seguridad from [citas].[FormularioSeguridad_Resultados]"
    df_IESS = pd.read_sql(queryIEES, cnxn)
    df_RegistroPenal=pd.read_sql(queryRegistroPenal, cnxn)
    df_queryRegistroSeguridad=pd.read_sql(queryRegistroSeguridad, cnxn)
    df=pd.merge(df_IESS,df_RegistroPenal, how="left")
    df=pd.merge(df,df_queryRegistroSeguridad, how="left")
    df= df.fillna("")



    
    result=df.to_json(orient="records",date_format="iso")
    parsed = json.loads(result)
    pd=json.dumps(parsed, indent=4) 

    

    return pd

#--------Prueba post--------------------
@app.route('/insertseguridad', methods=['POST'])
def registrar_curso():
    try:
        cursor = cnxn.cursor()
        sql="""insert into citas.FormularioSeguridad_Resultados (cedula,nombre,fecha,cargo,cd)
                values ('{0}','{1}','{2}','{3}','{4}')""".format(request.json['cedula'],request.json['nombre'],request.json['fecha'],request.json['cargo'],request.json['cd'])
        cursor.execute(sql)
        cnxn.commit()
        return jsonify({'mensaje':"registro exitoso"})
    except Exception as ex:
        return jsonify({'mensaje':"Error"})

@app.route('/actualizacionseguridad', methods=['POST'])
def evaluacion_curso():
    try:
        cursor = cnxn.cursor()
        sql="""UPDATE citas.FormularioSeguridad_Resultados 
        SET pregunta1 = '{0}', pregunta2 = '{1}',pregunta3 = '{2}',pregunta4 = '{3}',pregunta5 = '{4}',pregunta6 = '{5}',pregunta7 = '{6}',pregunta8 = '{7}',pregunta9 = '{8}',pregunta10 = '{9}',calificacion='{10}',estado='{11}',fechaIngreso='{12}'
        WHERE cedula = '{13}'""".format(request.json['pregunta1'],request.json['pregunta2'],request.json['pregunta3'],request.json['pregunta4'],request.json['pregunta5'],request.json['pregunta6'],request.json['pregunta7'],request.json['pregunta8'],request.json['pregunta9'],request.json['pregunta10'],request.json['calificacion'],request.json['estado'],request.json['fechaIngreso'],request.json['cedula'])
        cursor.execute(sql)
        cnxn.commit()
        return jsonify({'mensaje':"Curso registrado"})
    except Exception as ex:
        return jsonify({'mensaje':"Error"})


@app.route('/ConsultaPrincipal', methods=['GET'])
def ConsultaPrincipal():
    import pandas as pd

    cursor = cnxn.cursor()
    queryIEES = "SELECT TOP (20) * FROM [citas].[PRTAL_Transportistas] ORDER BY Fecha_hora_sistema DESC"
    queryRegistroPenal = "SELECT * FROM [citas].[RegistroPenal]"
    queryRegistroSeguridad = "Select cedula as Cedula,estado as Examen_seguridad from [citas].[FormularioSeguridad_Resultados]"
    df_IESS = pd.read_sql(queryIEES, cnxn)
    df_RegistroPenal=pd.read_sql(queryRegistroPenal, cnxn)
    df_queryRegistroSeguridad=pd.read_sql(queryRegistroSeguridad, cnxn)
    df=pd.merge(df_IESS,df_RegistroPenal, how="left")
    df=pd.merge(df,df_queryRegistroSeguridad, how="left")
    df= df.fillna("")
    result=df.to_json(orient="records",date_format="iso")
    parsed = json.loads(result)
    pd=json.dumps(parsed, indent=4) 
    return pd


@app.route('/Eppequiposactivos')
def Eppequiposactivos():
    import pandas as pd
    cursor = cnxn.cursor()
    queryEPP = "select [Nombres],[Apellido],[epp].[Inventario].[Cedula],[FechaCompra],[FechaRenovar],[NombreEpp],[Estado] FROM [epp].[Inventario] right JOIN [epp].[Colaboradores] ON [epp].[Inventario].[Cedula] = [epp].[Colaboradores].[Cedula]"
    df_epp = pd.read_sql(queryEPP, cnxn) 
    result=df_epp.to_json(orient="records",date_format="iso")
    parsed = json.loads(result)
    pd=json.dumps(parsed, indent=4) 
    return pd



@app.route('/EppequiposRenovar')
def Eppequiposrenovar():
    import pandas as pd
    cursor = cnxn.cursor()
    queryEPP = "select [ID],[Nombres],[Apellido],[epp].[Inventario].[Cedula],[FechaCompra],[FechaRenovar],[NombreEpp],[Estado] FROM [epp].[Inventario] right JOIN [epp].[Colaboradores] ON [epp].[Inventario].[Cedula] = [epp].[Colaboradores].[Cedula] where Estado='Renovar'"
    df_epp = pd.read_sql(queryEPP, cnxn) 
    result=df_epp.to_json(orient="records",date_format="iso")
    parsed = json.loads(result)
    pd=json.dumps(parsed, indent=4) 
    return pd


@app.route('/EppequiposRenovarsinAsignar')
def Eppequiposrenovarsinasignar():
    import pandas as pd
    cursor = cnxn.cursor()
    queryEPP = "Select [ID],[NombreEpp],[FechaCompra],[Estado] FROM [epp].[Inventario] where Estado='Sin asignar'"
    df_epp = pd.read_sql(queryEPP, cnxn) 
    result=df_epp.to_json(orient="records",date_format="iso")
    parsed = json.loads(result)
    pd=json.dumps(parsed, indent=4) 
    return pd

@app.route('/EppequiposUpdateRenovar', methods=['POST'])
def EppequiposUpdateRenovar():
    try:
        cursor = cnxn.cursor()
        sql="""UPDATE [epp].[Inventario]
SET NombreEpp = '{0}', Estado = '{1}',Cedula = '{2}',FechaRenovar = '{3}',FechaDeEntrega = '{4}'
WHERE [ID] = '{5}'""".format(request.json['NombreEpp'],request.json['Estado'],request.json['Cedula'],request.json['FechaRenovar'],request.json['FechaDeEntrega'],request.json['ID'])
        cursor.execute(sql)
        cnxn.commit()
        return jsonify({'mensaje':"Registro renovar exitoso"})
    except Exception as ex:
        return jsonify({'mensaje':"Error"})
    

@app.route('/EppequiposRenovarBaja', methods=['POST'])
def EppequiposRenovarBaja():
    try:
        cursor = cnxn.cursor()
        sql="""UPDATE [epp].[Inventario]
SET Estado = '{0}',Fechabaja= '{1}'
WHERE [ID] = '{2}'""".format(request.json['Estado'],request.json['Fechabaja'],request.json['ID'])
        cursor.execute(sql)
        cnxn.commit()
        return jsonify({'mensaje':"Registro renovar exitoso"})
    except Exception as ex:
        return jsonify({'mensaje':"Error"})
    
@app.route('/insertequiposEpp', methods=['POST'])
def insertequiposEpp():
    try:
        cursor = cnxn.cursor()
        sql="""insert into [epp].[Inventario] (NombreEpp,FechaCompra,Estado,Cedula,FechaRenovar)
                values ('{0}','{1}','{2}','{3}','{4}')""".format(request.json['NombreEpp'],request.json['FechaCompra'],request.json['Estado'],request.json['Cedula'],request.json['FechaRenovar'])
        cursor.execute(sql)
        cnxn.commit()
        return jsonify({'mensaje':"registro exitoso"})
    except Exception as ex:
        return jsonify({'mensaje':"Error"})

@app.route('/insertCoaborador', methods=['POST'])
def insertCoaborador():
    try:
        cursor = cnxn.cursor()
        sql="""insert into [epp].[Inventario] (NombreEpp,FechaCompra,Estado,Cedula,FechaRenovar)
                values ('{0}','{1}','{2}','{3}','{4}')""".format(request.json['NombreEpp'],request.json['FechaCompra'],request.json['Estado'],request.json['Cedula'],request.json['FechaRenovar'])
        cursor.execute(sql)
        cnxn.commit()
        return jsonify({'mensaje':"registro exitoso"})
    except Exception as ex:
        return jsonify({'mensaje':"Error"})











#Prueba
""" if __name__ == "__main__":

    app.run(debug=True) """

#Productivo
if __name__ == "__main__":

    serve(app, host='0.0.0.0',
            port=8080,
            threads=2      
            )

