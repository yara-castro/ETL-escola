import pandas as pd

# ====================
#  Etapa 1 - Extração
# ====================
def extrair_alunos(caminho_csv):
    df = pd.read_csv(caminho_csv)
    return df


# =========================
#  Etapa 2 - Transformação
# =========================
def definir_situacao(nota, frequencia):
    if nota >= 7 and frequencia >= 75:
        return "Aprovado"
    elif nota >= 5 and frequencia >= 75:
        return "Recuperação"
    else:
        return "Reprovado"


def gerar_mensagem(nome, situacao):
    if situacao == "Aprovado":
        return f"Parabéns {nome}! Continue com o ótimo desempenho."
    elif situacao == "Recuperação":
        return f"{nome}, atenção! É hora de reforçar os estudos."
    else:
        return f"{nome}, procure a coordenação para orientação pedagógica."


def transformar_dados(df):
    df["situacao"] = df.apply(
        lambda row: definir_situacao(row["nota"], row["frequencia"]),
        axis=1
    )

    df["mensagem"] = df.apply(
        lambda row: gerar_mensagem(row["nome"], row["situacao"]),
        axis=1
    )

    return df

# ========================
#  Etapa 3 - Carregamento
# ========================
def carregar_dados(df, caminha_saida):
    df.to_csv(caminha_saida, index=False)


# =======================
# Etapa Final - Execução
# =======================
def main():
    caminho_entrada = "alunos.csv"
    caminho_saida = "alunos_processados.csv"

    print("Iniciando ETL...")

    dados = extrair_alunos(caminho_entrada)
    dados_transformados = transformar_dados(dados)
    carregar_dados(dados_transformados, caminho_saida)

    print("ETL finalizado com sucesso!")


if __name__ == "__main__":
    main()
    