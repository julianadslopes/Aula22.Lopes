# pip install pandas sqlalchemy pymysql
import os
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from dotenv import load_dotenv

os.system("cls")

try:
# Carreha variáveis de ambiente do arquivo .env
    load_dotenv()

# Obtendo variáveis de ambiente    
    host = os.getenv('DB_HOST')
    port = os.getenv ("DB_PORT")
    user = os.getenv ("DB_USER")
    password = os.getenv ("DB_PASSWORD")
    database = os.getenv ("DB_DATABASE")

except Exception as e:
    print (f'Erro ao obter dados: {e}')
    exit()


try:
    print ("Obtendo dados...")
    host = "localhost"
    user = "root"
    password = "root"
    database = "bd_roubo_celular"

    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

    # CARREGANdO AS TABELAS
    df_base1 = pd.read_sql('basedp', engine)
    # print (df_base1.head())

    df_base2 = pd.read_sql ("basedp_roubo_celular", engine)
    # print (df_base2.head())

    # JUNTANDO TABELAS
    df_roubo = pd.merge(df_base1, df_base2, on="cod_ocorrencia", how="inner")
    # print (df_roubo.head())
    print(50*"*")
except Exception as e:
    print (f'Erro ao obter dados: {e}')
    exit()


try:
    print ("Carregando dados...")
# Delimitando as variáveis
    df_roubo_celular = df_roubo[["cod_ocorrencia", "ano", "aisp", "roubo_celular"]]
   
# Filtrando por ano 
    df_roubo_celular = df_roubo_celular[(df_roubo_celular["ano"]>=2022) & (df_roubo_celular["ano"]<=2023)]
    # print(df_roubo_celular.head())

# Totalizando recuperação de veículos por batalhão
    df_roubo_celular = df_roubo_celular.groupby(["aisp"]).sum(["roubo_celular"]).reset_index()

    print(df_roubo_celular.head())
    print("\nDados obtidos com sucesso!")    

except Exception as e:
    print (f'Erro ao obter dados: {e}')
    exit()


# Análises
try:
    print(50*"*")
    print ("Analisando os dados...")
#Array NumPy
    array_roubo_celular = np.array(df_roubo_celular ["roubo_celular"])

# Média de roubos
    media_roubo_celular = np.mean(array_roubo_celular)

# Mediana de roubos
    mediana_roubo_celular = np.median(array_roubo_celular)

# Distância entre média e mediana para ver se o valor da média é aceitável
    distancia_media_mediana = abs (media_roubo_celular-mediana_roubo_celular)/mediana_roubo_celular    

# Amplitude total: Quanto mais próximo de zero, maior a homegeneidade dos dados
    maximo = np.max (array_roubo_celular)
    minimo = np.min (array_roubo_celular)
    amplitude = maximo - minimo

# Quartis - método weibull
    q1 = np.quantile(array_roubo_celular, 0.25, method="weibull")
    q2 = np.quantile(array_roubo_celular, 0.50, method="weibull")
    q3 = np.quantile(array_roubo_celular, 0.75, method="weibull")
    iqr = q3-q1
    lim_superior = q3 + (1.5*iqr)
    lim_inferior = q1 - (1.5*iqr)

# Filtrando os outliers
    # Inferiores
    df_roubo_celular_outliers_inferiores = df_roubo_celular[df_roubo_celular["roubo_celular"]< lim_inferior]
    if len (df_roubo_celular_outliers_inferiores) == 0:
        print ("\nNão há outliers inferiores")
    else:    
        print ("\nOutlier inferior:")
        print (df_roubo_celular_outliers_inferiores.head())

    # Superiores
    df_roubo_celular_outliers_superiores = df_roubo_celular[df_roubo_celular["roubo_celular"]> lim_superior]
    if len (df_roubo_celular_outliers_superiores) == 0:
        print ("\nNão há outliers superiores")
    else:    
        print ("\nOutliers superiores: ")
        print (df_roubo_celular_outliers_superiores.head())

 # Batalhões com maior registro de roubo
    df_roubos_acima_q3 = df_roubo_celular[df_roubo_celular["roubo_celular"] > q3]
 # Batalhões com menores registro de roubo
    df_roubos_abaixo_q1 = df_roubo_celular[df_roubo_celular["roubo_celular"] < q1]


# PRINTS    
    print (f'\nMédia: {media_roubo_celular:.2f}')
    print (f'Mediana: {mediana_roubo_celular:.2f}')
    print (f'Distância entre a média e a mediana: {distancia_media_mediana:.2f}')
    print (f'Amplitude total:{amplitude:.2f}')
    print (f'Mínimo: {minimo}')
    print (f'Limite Inferior: {lim_inferior}')
    print (f'Q1: {q1}')
    print (f'Q2: {q2}')
    print (f'Q3: {q3}')
    print (f'IQR: {iqr}')
    print (f'Limite Superior: {lim_superior}')
    print (f'Máximo: {maximo}')

    print ("\nBatalhões com menores registros de roubos")
    print (df_roubos_abaixo_q1.sort_values(by="roubo_celular", ascending=True).head())
    print ("\nBatalhões com maiores registros de roubos")
    print (df_roubos_acima_q3.sort_values(by="roubo_celular", ascending=False).head())

except Exception as e:
    print (f'Erro ao obter dados: {e}')
    exit()

try:
    print(50*"*")
    print("Calculando Medidas de Distribuição")

#Calculando assimetria
    assimetria = df_roubo_celular['roubo_celular'].skew()

#Calculando curtose
    curtose = df_roubo_celular["roubo_celular"].kurtosis()

    print ('\nMedidas de Distribuição: ')
    print(f'Assimetria: {assimetria:.2f}')
    print(f'Curtose: {curtose:.2f}')
except ImportError as e:
    print (f'Erro ao visualizar dados: {e}')
    exit()

try:
    print(50*"*")
    print("\nVisualizando os dados")
    
    # Criando o layout da figura
    fig, axes = plt.subplots(2, 2, figsize=(18, 6))

    # Subplot 1: Histograma
    plt.subplot(2, 2, 1)
    plt.hist(array_roubo_celular, bins=10, edgecolor='skyblue', label= "Frequência")
    plt.axvline(media_roubo_celular, color ="g", linestyle="-", linewidth=1, label = "Média")
    plt.axvline(mediana_roubo_celular, color="y", linewidth=1, label = "Mediana")
    plt.title("Roubo de celulares 2022-2023", fontsize=14)
    plt.xlabel("Roubo de celular")
    plt.ylabel("Frequência")
    plt.legend()

    # Subplot 2: Boxplot 
    plt.subplot(2,2,2)
    plt.boxplot(array_roubo_celular, vert=False, showmeans=True)
    plt.title("Boxplot dos Dados")


   # Subplot 3: Quadro com Medidas Estatísticas

    plt.subplot(2, 2, 3)  # Configurar o segundo gráfico no lado direito
    plt.title("Medidas Estatísticas e de Distribuição")
    plt.text(0.1, 1.0, f'Média: {media_roubo_celular:.2f}', fontsize=12)
    plt.text(0.1, 0.9, f'Mediana: {mediana_roubo_celular:.2f}', fontsize=12)
    plt.text(0.1, 0.8, f'Distância: {distancia_media_mediana:.2f}', fontsize=12)
    plt.text(0.1, 0.7, f'Menor valor: {minimo}', fontsize=12) 
    plt.text(0.1, 0.6, f'Limite inferior: {lim_inferior:.2f}', fontsize=12)
    plt.text(0.1, 0.5, f'Q1: {q1}' f'  Q3: {q3}', fontsize=12)
    plt.text(0.1, 0.4, f'IQR: {iqr}', fontsize=12)
    plt.text(0.1, 0.3, f'Limite superior: {lim_superior}', fontsize=12)
    plt.text(0.1, 0.2, f'Maior valor: {maximo}', fontsize=12)
    plt.text(0.1, 0.1, f'Amplitude Total: {amplitude}', fontsize=12)
    plt.text(0.1, 0.0, f'Assimetria: {assimetria:.2f}' f'  Curtose: {curtose:.2f}', fontsize=12)
    plt.axis("off")

   # Subplot 3: Respostas

    plt.subplot(2, 2, 4)  # Configurar o segundo gráfico no lado direito
    plt.title("Respostas")
    plt.text(0.1, 0.9, "Existem BPMs com registros muito acima do normal? Sim, BPM 20 com 3055 registros", fontsize=12)
    plt.text(0.1, 0.7, 'Média confiável para representar os registros? Sim, pela proximidade da mediana e baixa assimetria', fontsize=12)
    plt.text(0.1, 0.5, 'Padrão de registros identificado? Leve assimetria positiva, indicando concentração abaixo da média', fontsize=12)
    plt.axis("off")

    # Ajustando o layout da figura
    plt.tight_layout()
    plt.show()


except ImportError as e:
    print (f'Erro ao visualizar dados: {e}')
    exit()               


print("Respondendo as Perguntas: ")
