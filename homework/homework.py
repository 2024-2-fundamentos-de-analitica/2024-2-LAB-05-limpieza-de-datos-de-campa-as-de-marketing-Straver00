# pylint: disable=import-outside-toplevel

import pandas as pd
import zipfile
import os
from datetime import datetime

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortgage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaign_contacts
    - previous_outcome: cambiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months

    """

    # Directorios de entrada y salida
    input_dir = "files/input/"
    output_dir = "files/output/"
    os.makedirs(output_dir, exist_ok=True)

    # Archivos de salida
    client_file = os.path.join(output_dir, "client.csv")
    campaign_file = os.path.join(output_dir, "campaign.csv")
    economics_file = os.path.join(output_dir, "economics.csv")

    # Inicialización de dataframes
    client_data = []
    campaign_data = []
    economics_data = []

    # Funciones para transformar columnas
    def transform_client_data(row):
        return {
            "client_id": row["client_id"],
            "age": row["age"],
            "job": row["job"].replace(".", "").replace("-", "_"),
            "marital": row["marital"],
            "education": row["education"].replace(".", "_") if row["education"] != "unknown" else pd.NA,
            "credit_default": 1 if row["credit_default"] == "yes" else 0,
            "mortgage": 1 if row["mortgage"] == "yes" else 0,
        }

    def transform_campaign_data(row):
        last_contact_day = datetime.strptime(f"2022-{row['month']}-{row['day']}", "%Y-%b-%d").strftime("%Y-%m-%d")
        return {
            "client_id": row["client_id"],
            "number_contacts": row["number_contacts"],
            "contact_duration": row["contact_duration"],
            "previous_campaign_contacts": row["previous_campaign_contacts"],
            "previous_outcome": 1 if row["previous_outcome"] == "success" else 0,
            "campaign_outcome": 1 if row["campaign_outcome"] == "yes" else 0,
            "last_contact_date": last_contact_day,
        }

    def transform_economics_data(row):
        return {
            "client_id": row["client_id"],
            "cons_price_idx": row["cons_price_idx"],
            "euribor_three_months": row["euribor_three_months"],
        }

    # Procesamiento de los archivos comprimidos
    for file_name in os.listdir(input_dir):
        if file_name.endswith(".zip"):
            with zipfile.ZipFile(os.path.join(input_dir, file_name), 'r') as zip_ref:
                for csv_file in zip_ref.namelist():
                    with zip_ref.open(csv_file) as f:
                        df = pd.read_csv(f)

                        # Imprimir las columnas disponibles para depuración
                        print(f"Procesando archivo: {csv_file}")
                        print(f"Columnas disponibles: {df.columns.tolist()}")

                        # Validar las columnas disponibles antes de procesar
                        if all(col in df.columns for col in ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]):
                            client_subset = df[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]]
                            client_data.extend(client_subset.apply(transform_client_data, axis=1))

                        if all(col in df.columns for col in ["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"]):
                            campaign_subset = df[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts", "previous_outcome", "campaign_outcome", "day", "month"]]
                            campaign_data.extend(campaign_subset.apply(transform_campaign_data, axis=1))

                        if all(col in df.columns for col in ["client_id", "cons_price_idx", "euribor_three_months"]):
                            economics_subset = df[["client_id", "cons_price_idx", "euribor_three_months"]]
                            economics_data.extend(economics_subset.apply(transform_economics_data, axis=1))

    # Guardar los datos procesados en los archivos de salida
    if client_data:
        pd.DataFrame(client_data).to_csv(client_file, index=False)
    if campaign_data:
        pd.DataFrame(campaign_data).to_csv(campaign_file, index=False)
    if economics_data:
        pd.DataFrame(economics_data).to_csv(economics_file, index=False)

    print("Procesamiento completado. Archivos generados en la carpeta files/output/.")

if __name__ == "__main__":
    clean_campaign_data()

