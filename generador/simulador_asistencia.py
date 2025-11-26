# ESTE CODIGO SWE EJECUTO EN LA MAQUINA VIRTUAL QUE SE VE EN LA FOTO DE ESTA SUBCARPETA

import pandas as pd
import numpy as np
import json
import random
import datetime
import time
import boto3
import sys

KINESIS_DATA_STREAM_NAME = "flujo-asistencia"
SEGUNDOS_ENTRE_EVENTOS = 20



def cargar_contexto():
    try:
        grupos_df = pd.read_csv("grupos_historicos.csv")
        inscripciones_df = pd.read_csv("inscripciones.csv")
        alumnos_df = pd.read_csv("alumnos.csv")
    except FileNotFoundError:
        print("Error: Faltan archivos CSV (grupos, inscripciones o alumnos).")
        print("Asegúrate de ejecutar data_generator.py primero.")
        sys.exit(1)

    alumnos_activos = alumnos_df[alumnos_df['estado'] == 'Activo']
    inscripciones_df = inscripciones_df[inscripciones_df['alumno_id'].isin(alumnos_activos['alumno_id'])]

    ultimo_semestre = grupos_df['semestre_historico'].max()
    print(f"Simulando asistencia para el semestre: {ultimo_semestre}")

    grupos_activos_df = grupos_df[grupos_df['semestre_historico'] == ultimo_semestre]
    inscripciones_activas_df = inscripciones_df[inscripciones_df['grupo_id'].isin(grupos_activos_df['grupo_id'])]

    contexto_df = inscripciones_activas_df.merge(grupos_activos_df, on="grupo_id", how="left")

    print(f"Contexto cargado: {len(contexto_df)} inscripciones válidas (Alumno Activo + Grupo Actual).")
    return contexto_df


def aplicar_sesgo_asistencia(horario_json_str):
    horario_dict = json.loads(horario_json_str)
    horas_inicio = [int(v.split('-')[0]) for v in horario_dict.values()]
    hora_inicio_promedio = np.mean(horas_inicio)

    prob_ausencia = 0.05
    if hora_inicio_promedio <= 7:
        prob_ausencia = 0.2
    elif hora_inicio_promedio >= 17:
        prob_ausencia = 0.35

    evento = "Entrada"
    if np.random.rand() < prob_ausencia:
        evento = "Ausencia"

    return evento, hora_inicio_promedio


def simular_flujo(contexto_df, kinesis_client):
    print(f"\n--- Iniciando simulación de streaming hacia Kinesis Data Stream: {KINESIS_DATA_STREAM_NAME} ---")

    cola_de_envio = contexto_df.sample(frac=1).reset_index(drop=True)
    total_registros = len(cola_de_envio)

    print(f"Total de eventos a enviar: {total_registros}")
    print(f"Tiempo estimado: {(total_registros * SEGUNDOS_ENTRE_EVENTOS) / 60:.2f} minutos.")
    print(f"Generando un evento cada {SEGUNDOS_ENTRE_EVENTOS} segundos. Presiona CTRL+C para detener antes.")

    asistencia_id_counter = 9000000

    for index, inscripcion in cola_de_envio.iterrows():
        try:
            evento, hora_base = aplicar_sesgo_asistencia(inscripcion['horario_json'])
            ahora = datetime.datetime.now()

            if evento == "Entrada":
                hora_llegada = int(hora_base) + np.random.uniform(0, 0.3)
                fecha_hora_evento = ahora.replace(hour=int(hora_llegada), minute=int((hora_llegada % 1) * 60), second=0,
                                                  microsecond=0)
            else:
                fecha_hora_evento = ahora.replace(hour=int(hora_base), minute=0, second=0, microsecond=0)

            payload = {
                "asistencia_id": f"AS{asistencia_id_counter}",
                "alumno_id": inscripcion['alumno_id'],
                "grupo_id": inscripcion['grupo_id'],
                "fecha_hora_evento": fecha_hora_evento.isoformat(),
                "tipo_evento": evento,
                "timestamp_ingesta": ahora.isoformat()
            }

            data_bytes = (json.dumps(payload)).encode('utf-8')

            kinesis_client.put_record(
                StreamName=KINESIS_DATA_STREAM_NAME,
                Data=data_bytes,
                PartitionKey=payload['alumno_id']
            )

            print(
                f"[{index + 1}/{total_registros}] Enviado: {payload['asistencia_id']} - {payload['alumno_id']} ({payload['tipo_evento']})")

            asistencia_id_counter += 1
            time.sleep(SEGUNDOS_ENTRE_EVENTOS)

        except KeyboardInterrupt:
            print("\nSimulación detenida manualmente por el usuario.")
            return
        except Exception as e:
            print(f"Error enviando registro: {e}")
            break

    print("\n--- Simulación finalizada. Se han enviado todos los registros únicos. ---")


if __name__ == "__main__":
    print("Iniciando Simulador de Asistencia...")

    try:
        kinesis_client = boto3.client('kinesis', region_name='us-east-1')
        kinesis_client.list_streams(Limit=1)
        print("Conexión con AWS Kinesis Data Streams exitosa.")
    except Exception as e:
        print(f"Error fatal: No se pudo conectar a AWS Kinesis.")
        print(f"Error: {e}")
        sys.exit(1)

    contexto_df = cargar_contexto()

    if not contexto_df.empty:
        simular_flujo(contexto_df, kinesis_client)
    else:
        print("Error: No se encontraron inscripciones activas para simular.")